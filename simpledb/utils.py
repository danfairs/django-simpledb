def domain_for_model(model):
    return '%s.%s' % (model._meta.app_label, model.__name__)