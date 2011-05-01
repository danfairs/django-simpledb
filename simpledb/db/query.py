from boto.sdb.db.query import Query as BotoQuery
from boto.sdb.db.property import Property

from simpledb.utils import domain_for_model

def model_adapter(django_model):
    properties = {}
    for field in django_model._meta.fields:
        default = field.default
        if callable(default):
            default = default()
        choices = [c[0] for c in getattr(field, 'choices', ())]
        properties[field.name] = Property(
            verbose_name=field.verbose_name,
            name=field.name,
            default=default,
            required=not field.null,
            choices=choices,
            unique=field.unique
        )

    class ModelAdapter(django_model):
        """ Adapter to provide the API that boto expects its models to have for
        normal Django models
        """

        # Used by SDBManager._get_all_descendents. Might need to implement
        # this for model inheritance...
        __sub_classes__ = ()

        # Override __name__ as boto uses this to figure out the
        # __type__ attributes.
        __name__ = domain_for_model(django_model)

        @classmethod
        def find_property(cls, prop_name):
            """ Find the named property. Returns None if the property can't
            be found
            """
            return properties.get(prop_name)

    return ModelAdapter

class SimpleDBQuery(BotoQuery):

    def __init__(self, manager, model, limit=None, next_token=None):
        self.manager = manager
        self.model_class = model_adapter(model)
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
        raise NotImplementedError
