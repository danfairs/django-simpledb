import mock
import unittest
from django.db import models

class M(models.Model):
    name = models.CharField(
        'long name',
        max_length=20,
        default='hi',
        unique=True)


class ModelAdapterTests(unittest.TestCase):

    def adapt(self, model):
        from simpledb.query import model_adapter
        return model_adapter(model)

    def test_find_property_ok(self):
        """ find_property should return a boto Property object for fields
        present on the model
        """
        m = self.adapt(M)
        prop = m.find_property('name')
        self.assertEqual('long name', prop.verbose_name)
        self.assertEqual(True, prop.unique)
        self.assertEqual('hi', prop.default)

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
        self.assertEqual(32, len(r))
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


class InsertCompilerTests(unittest.TestCase):

    def setUp(self):
        self.query = mock.Mock()
        self.model = self.query.model = M
        meta = mock.Mock()
        self.query.get_meta.return_value = meta
        meta.pk.column = 'id_col'
        self.connection = mock.Mock()

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


class QueryTests(unittest.TestCase):

    def query(self):
        from simpledb.query import SimpleDBQuery
        return SimpleDBQuery(None, M, None)

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
