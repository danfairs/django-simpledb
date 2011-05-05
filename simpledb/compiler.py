import datetime
import logging
import sys
import uuid

from django.db.models.sql.constants import LOOKUP_SEP, MULTI, SINGLE
from django.db.models.sql.where import AND, OR
from django.db.utils import DatabaseError, IntegrityError
from django.utils.tree import Node

from functools import wraps

from boto.exception import (BotoClientError, SDBPersistenceError,
    BotoServerError)
from boto.sdb.domain import Domain

from djangotoolbox.db.basecompiler import NonrelQuery, NonrelCompiler, \
    NonrelInsertCompiler, NonrelUpdateCompiler, NonrelDeleteCompiler

from simpledb.query import SimpleDBQuery
from simpledb.utils import domain_for_model

logger = logging.getLogger('simpledb')
AWS_MAX_RESULT_SIZE = 2500

# TODO: Change this to match your DB
# Valid query types (a dictionary is used for speedy lookups).
OPERATORS_MAP = {
    'exact': '=',
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<=',
    'isnull': lambda lookup_type, value: ('=' if value else '!=', None),

    #'startswith': lambda lookup_type, value: ...,
    #'range': lambda lookup_type, value: ...,
    #'year': lambda lookup_type, value: ...,
}

NEGATION_MAP = {
    'exact': '!=',
    'gt': '<=',
    'gte': '<',
    'lt': '>=',
    'lte': '>',
    'isnull': lambda lookup_type, value: ('!=' if value else '=', None),

    #'startswith': lambda lookup_type, value: ...,
    #'range': lambda lookup_type, value: ...,
    #'year': lambda lookup_type, value: ...,
}

DATETIME_ISO8601 = '%Y-%m-%dT%H:%M:%S.%f'
DATE_ISO8601 = '%Y-%m-%d'

def safe_call(func):
    @wraps(func)
    def _func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (BotoClientError, SDBPersistenceError,
            BotoServerError), e:
            raise DatabaseError, DatabaseError(*tuple(e)), sys.exc_info()[2]
    return _func

def save_entity(connection, model, data):
    domain_name = domain_for_model(model)
    manager = connection.create_manager(domain_name)
    attrs = {
        '__type__': domain_name,
    }
    attrs.update(data)
    if not attrs.has_key('_id'):
        # New item. Generate an ID.
        attrs['_id'] = uuid.uuid4().int
    domain = Domain(name=domain_name, connection=manager.sdb)
    domain.put_attributes(attrs['_id'], attrs, replace=True)
    return attrs['_id']


class BackendQuery(NonrelQuery):

    def __init__(self, compiler, fields):
        super(BackendQuery, self).__init__(compiler, fields)
        # TODO: add your initialization code here
        domain = domain_for_model(self.query.model)
        self.db_query = SimpleDBQuery(
            self.connection.create_manager(domain), self.query.model)

    # This is needed for debugging
    def __repr__(self):
        # TODO: add some meaningful query string for debugging
        return '<BackendQuery: %s>' % self.query.model._meta.db_table

    @safe_call
    def fetch(self, low_mark=None, high_mark=None):
        reslice = 0
        if high_mark > AWS_MAX_RESULT_SIZE:
            logger.warn('Requested result size %s, assuming infinite' % (
                high_mark))
            reslice = high_mark
            high_mark = None
        # TODO: run your low-level query here
        #low_mark, high_mark = self.limits
        if high_mark is None:
            # Infinite fetching
            results = self.db_query.fetch_infinite(offset=low_mark)
            if reslice:
                results = list(results)[:reslice]
        elif high_mark > low_mark:
            # Range fetching
            results = self.db_query.fetch_range(high_mark - low_mark, low_mark)
        else:
            results = ()

        for entity in results:
            entity[self.query.get_meta().pk.column] = entity['_id']
            del entity['_id']
            yield entity

    @safe_call
    def count(self, limit=None):
        # TODO: implement this
        return self.db_query.count(limit)

    @safe_call
    def delete(self):
        self.db_query.delete()

    @safe_call
    def order_by(self, ordering):
        # TODO: implement this
        for order in ordering:
            if order.startswith('-'):
                column, direction = order[1:], 'DESC'
            else:
                column, direction = order, 'ASC'
            if column == self.query.get_meta().pk.column:
                column = '_id'
            self.db_query.add_ordering(column, direction)

    # This function is used by the default add_filters() implementation which
    # only supports ANDed filter rules and simple negation handling for
    # transforming OR filters to AND filters:
    # NOT (a OR b) => (NOT a) AND (NOT b)
    @safe_call
    def add_filter(self, column, lookup_type, negated, db_type, value):
        # TODO: implement this or the add_filters() function (see the base
        # class for a sample implementation)

        # Emulated/converted lookups
        if column == self.query.get_meta().pk.column:
            column = '_id'

        # Special-case IN
        if lookup_type == 'in':
            if negated:
                # XXX needs testing, error for now.
                raise NotImplementedError
                #op = '!='
            else:
                op = '='
            # boto needs IN queries' values to be lists of lists.
            db_value = [[self.convert_value_for_db(db_type, v)] for v in value]
        else:
            if negated:
                try:
                    op = NEGATION_MAP[lookup_type]
                except KeyError:
                    raise DatabaseError("Lookup type %r can't be negated" % lookup_type)
            else:
                try:
                    op = OPERATORS_MAP[lookup_type]
                except KeyError:
                    raise DatabaseError("Lookup type %r isn't supported" % lookup_type)

            # Handle special-case lookup types
            if callable(op):
                op, value = op(lookup_type, value)

            db_value = self.convert_value_for_db(db_type, value)

        # XXX check this is right
        self.db_query.filter('%s %s' % (column, op), db_value)
        #self.db_query.filter(column, op, db_value)

class SQLCompiler(NonrelCompiler):
    query_class = BackendQuery

    # This gets called for each field type when you fetch() an entity.
    # db_type is the string that you used in the DatabaseCreation mapping
    def convert_value_from_db(self, db_type, value):
        # Handle list types
        if isinstance(value, (list, tuple)) and len(value) and \
                db_type.startswith('ListField:'):
            db_sub_type = db_type.split(':', 1)[1]
            value = [self.convert_value_from_db(db_sub_type, subvalue)
                     for subvalue in value]
        elif db_type == 'long':
            value = long(value)
        elif db_type == 'int':
            value = int(value)
        # Dates and datetimes are encoded as ISO 8601
        elif db_type == 'date':
            value = datetime.datetime.strptime(value, DATE_ISO8601).date()
        elif db_type == 'datetime':
            value = datetime.datetime.strptime(value, DATETIME_ISO8601)
        elif isinstance(value, str):
            # Always retrieve strings as unicode
            value = value.decode('utf-8')
        return value

    # This gets called for each field type when you insert() an entity.
    # db_type is the string that you used in the DatabaseCreation mapping
    def convert_value_for_db(self, db_type, value):
        # TODO: implement this

        if isinstance(value, unicode):
            value = unicode(value)
        elif isinstance(value, str):
            # Always store strings as unicode
            value = value.decode('utf-8')
        elif isinstance(value, (list, tuple)) and len(value) and \
                db_type.startswith('ListField:'):
            db_sub_type = db_type.split(':', 1)[1]
            value = [self.convert_value_for_db(db_sub_type, subvalue)
                     for subvalue in value]
        elif isinstance(value, datetime.datetime):
            value = value.strftime(DATETIME_ISO8601)
        elif isinstance(value, datetime.date):
            value = value.strftime(DATE_ISO8601)
        elif isinstance(value, (int, long)):
            value = str(value).decode('ascii')

        # XXX string quoting rules
        return value

# This handles both inserts and updates of individual entities
class SQLInsertCompiler(NonrelInsertCompiler, SQLCompiler):
    @safe_call
    def insert(self, data, return_id=False):
        pk_column = self.query.get_meta().pk.column
        if pk_column in data:
            data['_id'] = data[pk_column]
            del data[pk_column]
        pk = save_entity(self.connection, self.query.model, data)
        return pk

class SQLUpdateCompiler(NonrelUpdateCompiler, SQLCompiler):
    pass

class SQLDeleteCompiler(NonrelDeleteCompiler, SQLCompiler):
    pass
