from boto.sdb.db.query import Query as BotoQuery
from boto.sdb.db.property import Property
from boto.sdb.domain import Domain
from boto.sdb.item import Item
from simpledb.utils import domain_for_model

def property_from_field(field):
    default = field.default
    if callable(default):
        default = default()
    choices = [c[0] for c in getattr(field, 'choices', ())]
    return Property(
        verbose_name=field.verbose_name,
        name=field.name,
        default=default,
        required=not field.null,
        choices=choices,
        unique=field.unique
    )


def model_adapter(django_model, manager):
    """ Return a generated subclass of django_model that conforms to the
    API that boto expects of its own models.
    """
    class ModelAdapter(object):
        """ Adapter to provide the API that boto expects its models to have for
        normal Django models
        """
        def __new__(self, id, **params):
            domain_name = domain_for_model(self.model_class)
            domain = Domain(name=domain_name, connection=manager.sdb)
            item = Item(domain, id)
            params['_id'] = id
            item.update(params)
            return item
            #return self.model_class(**attrs)

        # Used by SDBManager._get_all_descendents. Might need to implement
        # this for model inheritance...
        __sub_classes__ = ()

        # Used by simpledb.base.SDBManager to track the real Django model
        # class this represents, and for us to know what kind of model to
        # actually instantiate.
        model_class = None

        @classmethod
        def find_property(cls, prop_name):
            """ Find the named property. Returns None if the property can't
            be found
            """
            result = None
            for field in django_model._meta.fields:
                if field.name == prop_name:
                    result = property_from_field(field)
                    break
            return result

        @classmethod
        def properties(cls, hidden=True):
            return [property_from_field(f) for f in django_model._meta.fields]

    ModelAdapter.model_class = django_model
    ModelAdapter.__name__ =  domain_for_model(django_model)
    return ModelAdapter


class SimpleDBQuery(BotoQuery):

    def __init__(self, manager, model, limit=None, next_token=None):
        self.manager = manager
        self.model_class = model_adapter(model, manager)
        self.limit = limit
        self.offset = 0
        self.filters = []
        self.select = None
        self.sort_by = None
        self.rs = None
        self.next_token = next_token

    def fetch_infinite(self, offset):
        return self.manager.query(self)

    def fetch_range(self, count, low_mark):
        self.fetch(offset=low_mark, limit=low_mark+count)
        return self.manager.query(self)

    def add_ordering(self, column, direction):
        if direction.lower() == 'desc':
            sort_by = '-%s' % column
        else:
            sort_by = column

        if self.sort_by and self.sort_by != sort_by:
            # XXX What should we do here? Order in software?
            raise NotImplementedError

        self.sort_by = sort_by

