from functools import partial
from graph import TestGraph


class UserAuth:
    def __init__(self, *args, **kwargs):
        self.email = kwargs.get('email')


def create_object(*args, **kwargs):
    if 'user_auth' in args:
        email = kwargs.get('email')
        user_auth = UserAuth(email=email)
        return user_auth

    return f'args={args}, kwargs={kwargs}'


class CreateUser(TestGraph):
    user_auth = (partial(create_object, 'user_auth'), 'email')
    user = (partial(create_object, 'user'), 'first_name', 'last_name',
            'user_auth.email', 'institution', 'groups')


test = CreateUser(email='test@example.com', first_name='Dopey',
                  last_name='Dwarf', institution='test institution',
                  groups=None)

# print(test._inputs)
# print(test._outputs)
# print(test._graph)

res = test.execute()

print(res)


@node
def create_project(first_name, last_name, email, groups, ):
    pass

class CreateRequest(FlowGraph):


class CreateBiospecimenRequest(FlowGraph):
    request = CreateRequest(project, request_type=BIOSPECIMEN)
    bleh = SomeStep('request.owner')

class CreateRequest(FlowGraph):
    project = CreateProject(owner, title, abstract)

class Example1(FlowGraph):
    c = Func1(a, b)
    e = Func2(c, d)

class Example2(FlowGraph):
    p = Example1(q, r)
    f = Func3(p.c, t)