""" actions.py """
from functools import partial

from django.contrib.contenttypes.models import ContentType

from .graph import FixtureGraph


def create_object(obj_type, *args, **kwargs):
    content_type = ContentType.objects.get(model=obj_type)
    model = content_type.model_class()
    instance = model(**kwargs)
    instance.save()

    return instance


class CreateUser(FixtureGraph):
    user_auth = (partial(create_object, 'user_auth'), 'email')
    user = (partial(create_object, 'user'), 'first_name', 'last_name',
            'user_auth', 'institutions', 'groups', 'globus_id')
    user_institution = (partial(create_object, 'userinstitution'), 'user',
                        'institution', ('primary', True))
    user_phone = (partial(create_object, 'userphone'), 'user', ('type', 3),
                  'phone_number')
    user_email = (partial(create_object, 'useremail'), 'user', 'email')
