import datetime
import mock
import unittest
from django.db import models

class M(models.Model):
    name = models.CharField(
        'long name',
        max_length=20,
        default='hi',
        unique=True)

class X(models.Model):
    fk = models.ForeignKey('M')

class ModelAdapterTests(unittest.TestCase):

    def adapt(self, model):
        from simpledb.query import model_adapter
        from boto.sdb.db import model as boto_model
        from boto.sdb.db.manager.sdbmanager import SDBManager

        manager = mock.Mock(spec=SDBManager)
        manager.sdb = self.sdb = mock.Mock(name='sdb')
        return model_adapter(model, manager)

    def test_find_property_ok(self):
        """ find_property should return a boto Property object for fields
        present on the model
        """
        m = self.adapt(M)
        prop = m.find_property('name')
        self.assertEqual('long name', prop.verbose_name)
        self.assertEqual(True, prop.unique)
        self.assertEqual('hi', prop.default)

    def test_find_property_fk(self):
        """ The name of the property should be the database column, else the
        foreign key values won't be populated.
        """
        m = self.adapt(X)
        prop = m.find_property('fk')
        self.assertEqual('fk_id', prop.name)

    def test_find_property_id(self):
        """ The _id property is special-cased to return the primary key
        """
        m = self.adapt(X)
        prop = m.find_property('_id')
        self.assertEqual('id', prop.name)

    def test_find_property_callable_default(self):
        """ If the default is callable, then accessing the default should
        call.
        """
        r = range(0, 3)
        def count():
            return r.pop(0)

        class N(models.Model):
            counter = models.PositiveIntegerField(default=count)
        m = self.adapt(N)
        self.assertEqual(0, m.find_property('counter').default)
        self.assertEqual(1, m.find_property('counter').default)
        self.assertEqual(2, m.find_property('counter').default)

    def test_missing_property_none(self):
        """ If the property is missing, we should get None back.
        """
        m = self.adapt(M)
        self.assertEqual(None, m.find_property('foo'))


class SaveEntityTests(unittest.TestCase):

    def setUp(self):
        from boto.sdb.db import model
        from boto.sdb.db.manager.sdbmanager import SDBManager
        self.manager = mock.Mock(spec=SDBManager)
        self.manager.sdb = self.sdb = mock.Mock(name='sdb')
        self.connection = mock.Mock()
        self.connection.create_manager.return_value = self.manager

    def save_entity(self, *args, **kwargs):
        from simpledb.compiler import save_entity
        return save_entity(*args, **kwargs)

    def test_save_entity_no_id(self):
        """ Check that the appropriate methods are invoked on the boto
        manager when no id is present """
        r = self.save_entity(self.connection, M, {'name': u'foo'})

        # Since our data didn't have an _id, we should get a new uuid4 ID back
        self.assertTrue(bool(r))
        args, kwargs = self.sdb.put_attributes.call_args
        self.assertEqual({}, kwargs)
        domain, id, data, replace, expected = args
        self.assertEqual('simpledb_m', domain.name)
        self.assertEqual(r, id)
        self.assertEqual({
            '_id': r,
            '__type__':
            'simpledb_m',
            'name': 'foo',
        }, data)
        self.assertTrue(replace)
        self.assertEqual(None, expected)

    def test_save_entity_with_id(self):
        """ Check that the appropriate methods are invoked on the boto
        manager when an id is present """
        my_id = u'x' * 32
        r = self.save_entity(self.connection, M, {
            'name': u'foo',
            '_id': my_id
        })

        # Shoudl get the same ID back
        self.assertEqual(my_id, r)
        args, kwargs = self.sdb.put_attributes.call_args
        self.assertEqual({}, kwargs)
        domain, id, data, replace, expected = args
        self.assertEqual('simpledb_m', domain.name)
        self.assertEqual(r, id)
        self.assertEqual({
            '_id': r,
            '__type__':
            'simpledb_m',
            'name': 'foo',
        }, data)
        self.assertTrue(replace)
        self.assertEqual(None, expected)


class ConnectionTests(unittest.TestCase):

    def setUp(self):
        self.query = mock.Mock()
        self.model = self.query.model = M
        meta = mock.Mock()
        self.query.get_meta.return_value = meta
        meta.pk.column = 'id_col'
        self.connection = mock.Mock()


class InsertConnectionTests(ConnectionTests):

    def compiler(self):
        from simpledb.compiler import SQLInsertCompiler
        return SQLInsertCompiler(self.query, self.connection, None)

    @mock.patch('simpledb.compiler.save_entity')
    def test_insert_compiler_no_id(self, mock_save):
        """ Check that the insert compiler invokes save_entity correctly,
        when there's no ID column present in the data
        """
        compiler = self.compiler()
        compiler.insert({'name': 'foo'})
        args, kwargs = mock_save.call_args
        conn, m, data = args
        self.assertEqual(self.connection, conn)
        self.assertEqual(self.model, m)
        self.assertEqual({'name': 'foo'}, data)

    @mock.patch('simpledb.compiler.save_entity')
    def test_insert_compiler_id(self, mock_save):
        """ Check that the insert compiler invokes save_entity correctly,
        when there's an ID column present in the data - it should get renamed
        to _id.
        """
        compiler = self.compiler()
        compiler.insert({
            'name': 'foo',
            'id_col': 'fizz',
        })
        args, kwargs = mock_save.call_args
        conn, m, data = args
        self.assertEqual(self.connection, conn)
        self.assertEqual(self.model, m)
        self.assertEqual({
            'name': 'foo',
            '_id': 'fizz'
        }, data)


class SQLConnectionTests(ConnectionTests):

    def compiler(self):
        from simpledb.compiler import SQLCompiler
        return SQLCompiler(self.query, self.connection, None)

    def test_convert_date_from_db(self):
        """ Check we can convert from an ISO 8601 format back to a
        datetime.date
        """
        dt = '2008-06-10'
        converted = self.compiler().convert_value_from_db('date', dt)
        self.assertEqual(2008, converted.year)
        self.assertEqual(6, converted.month)
        self.assertEqual(10, converted.day)

    def test_convert_date_to_db(self):
        """ Check that a date gets encoded to an ISO 8601 string
        correctly
        """
        dt = datetime.date(2008, 6, 10)
        actual = self.compiler().convert_value_for_db('date', dt)
        self.assertEqual('2008-06-10', actual)

    def test_convert_datetime_from_db(self):
        """ Check that datetimes get created properly from encoded strings
        """
        dt = '2008-06-10T14:02:36.25'
        converted = self.compiler().convert_value_from_db('datetime', dt)
        self.assertEqual(2008, converted.year)
        self.assertEqual(6, converted.month)
        self.assertEqual(10, converted.day)
        self.assertEqual(14, converted.hour)
        self.assertEqual(2, converted.minute)
        self.assertEqual(36, converted.second)
        self.assertEqual(250000, converted.microsecond)

    def test_convert_datetime_to_db(self):
        dt = datetime.datetime(2008, 6, 10, 14, 2, 36, 250000)
        actual = self.compiler().convert_value_for_db('datetime', dt)
        self.assertEqual('2008-06-10T14:02:36.250000', actual)

    def test_convert_long_to_db(self):
        actual = self.compiler().convert_value_for_db('long', 1L)
        self.assertEqual('1', actual)

    def test_convert_long_from_db(self):
        actual = self.compiler().convert_value_from_db('long', '1')
        self.assertEqual(1L, actual)

class SimpleDBQueryTests(unittest.TestCase):

    def query(self):
        from simpledb.query import SimpleDBQuery
        manager = mock.Mock()
        return SimpleDBQuery(manager, M, None)

    def test_ordering_asc(self):
        query = self.query()
        query.add_ordering('foo', 'ASC')
        self.assertEqual('foo', query.sort_by)

    def test_ordering_desc(self):
        query = self.query()
        query.add_ordering('foo', 'DESC')
        self.assertEqual('-foo', query.sort_by)

    def test_ordering_reset(self):
        query = self.query()
        query.add_ordering('foo', 'DESC')

        # Change ordering isn't implemented
        self.assertRaises(
            NotImplementedError,
            query.add_ordering,
            'foo',
            'ASC'
        )

        # Not changing, should be OK
        query.add_ordering('foo', 'DESC')

        # Change order field also not allowed
        self.assertRaises(
            NotImplementedError,
            query.add_ordering,
            'bar',
            'DESC'
        )

    @mock.patch('simpledb.query.SimpleDBQuery.fetch_infinite')
    @mock.patch('boto.sdb.domain.Domain.batch_delete_attributes')
    def test_delete(self, mock_boto_delete, mock_fetch):
        """ Delete should fetch all items in the current query, and end up
        calling boto's Domain.batch_delete_attributes with a dict - all item
        names as keys, and None as values. This will cause SimpleDB to delete
        the item completely.
        """
        fake_results = [{'_id': 1}, {'_id': 2}]
        mock_fetch.return_value = iter(fake_results)
        query = self.query()
        query.delete()
        mock_fetch.assert_called_with(0)
        mock_boto_delete.assert_called_with({1: None, 2: None})


class BackendQueryTests(unittest.TestCase):

    @mock.patch('simpledb.query.domain_for_model')
    def backend_query(self, mock_domain):
        from simpledb.compiler import BackendQuery
        mock_domain.return_value = 'some_name'
        compiler = mock.Mock()
        def f(db_type, value):
            return value
        compiler.convert_value_for_db.side_effect = f
        return BackendQuery(compiler, None)

    @mock.patch('simpledb.query.SimpleDBQuery.delete')
    def test_delete(self, mock_delete):
        """ delete() on the backend query simply proxies directly to the
        delete() method on SimpleDBQuery
        """
        self.backend_query().delete()
        mock_delete.assert_called_with()

    def test_add_filter_in(self):
        """ 'in' queries are passed to boto like regular equals queries, but
        with a list of values rather than a single value. boto will OR them.
        """
        query = self.backend_query()
        query.add_filter('name', 'in', False, 'unicode', ['x', 'y'])
        self.assertEqual([('name =', [['x'], ['y']])], query.db_query.filters)

class IntegrationTests(unittest.TestCase):

    @mock.patch('simpledb.query.SimpleDBQuery.fetch_infinite')
    def test_fetch(self, mock_fetch):
        """
        """
        # List of values for the mock fetch to return, as it'll be called
        # for both the fetch of X, and the traverse to the related M model.
        values = [
            [{
                '_id': u'999',
                'fk_id': u'123456'
            }],
            [{
                '_id': u'123456',
                'name': u'name for m'
            }]
        ]
        def r(*args, **kwargs):
            return values.pop(0)
        mock_fetch.side_effect = r
        xs = X.objects.all()
        self.assertEqual(1, len(xs))
        x = xs[0]
        self.assertEqual(123456, x.fk_id)
        self.assertEqual(u'name for m', x.fk.name)
