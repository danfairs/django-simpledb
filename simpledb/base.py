from djangotoolbox.db.base import NonrelDatabaseFeatures, \
    NonrelDatabaseOperations, NonrelDatabaseWrapper, NonrelDatabaseClient, \
    NonrelDatabaseValidation, NonrelDatabaseIntrospection, \
    NonrelDatabaseCreation

# We don't use this, but `model` needs to be imported first due to a
# relative import in boto.sdb.db.manager.get_manager, which is called in
# a metaclass. This would otherwise be called during our next import line,
# pulling in SDBManager, thus causing an ImportError due to a cyclic import.
from boto.sdb.db import model
from boto.sdb.db.manager.sdbmanager import SDBManager
import boto

from simpledb.utils import domain_for_model

class HasConnection(object):

    @property
    def sdb(self):
        if not hasattr(self, '_sdb'):
            settings = self.connection.settings_dict
            self._sdb = boto.connect_sdb(
                aws_access_key_id=settings['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=settings['AWS_SECRET_ACCESS_KEY'])
        return self._sdb

# TODO: You can either use the type mapping defined in NonrelDatabaseCreation
# or you can override the mapping, here:
class DatabaseCreation(NonrelDatabaseCreation, HasConnection):
    data_types = dict(NonrelDatabaseCreation.data_types, **{
        'TextField':                    'text',
        'XMLField':                     'text',
        'IntegerField':                 'long',
        'SmallIntegerField':            'int',
        'PositiveIntegerField':         'long',
        'PositiveSmallIntegerField':    'int',
        'BigIntegerField':              'long',
        'AutoField':                    'long',
        'DecimalField':                 'unicode',
        'ForeignKey':                   'long',
        'DateField':                    'date',
        'DateTimeField':                'datetime',
    })

    def sql_create_model(self, model, style, known_models=set()):
        """ We don't actually return any SQL here, but we do go right ahead
        and create a domain for the model.
        """
        domain_name = domain_for_model(model)
        self.sdb.create_domain(domain_name)
        return [], {}

    def create_test_db(self, verbosity=1, autoclobber=False):
        """ No test database for us """
        return ''

    def destroy_test_db(self, old_database_name, verbosity=1):
        """ No-op """
        pass


class DatabaseFeatures(NonrelDatabaseFeatures):
    pass

class DatabaseOperations(NonrelDatabaseOperations):
    compiler_module = __name__.rsplit('.', 1)[0] + '.compiler'

class DatabaseClient(NonrelDatabaseClient):
    pass

class DatabaseValidation(NonrelDatabaseValidation):
    pass

class DatabaseIntrospection(NonrelDatabaseIntrospection, HasConnection):

    def table_names(self):
        """ We map tables onto AWS domains.
        """
        rs = self.sdb.get_all_domains()
        return [d.name for d in rs]


class DatabaseWrapper(NonrelDatabaseWrapper):
    def __init__(self, *args, **kwds):
        super(DatabaseWrapper, self).__init__(*args, **kwds)
        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.validation = DatabaseValidation(self)
        self.introspection = DatabaseIntrospection(self)

    def create_manager(self, domain_name):
        return SDBManager(cls=None, db_name=domain_name,
            db_user=self.settings_dict['AWS_ACCESS_KEY_ID'],
            db_passwd=self.settings_dict['AWS_SECRET_ACCESS_KEY'],
            db_host=None, db_port=None, db_table=None, ddl_dir=None,
            enable_ssl=True)
