import os
import requests

# definition of the API address
api_address = os.environ.get('API_ADR')

# definition of the API address
api_address = os.environ.get('API_ADR')
# API port
api_port = 8000

# request alice user
r = requests.get(
    url='http://{address}:{port}/permissions'.format(address=api_address, port=api_port),
    params= {
        'username': 'alice',
        'password': 'wonderland'
    }
)
output = '''
============================
    Authentication test
============================
request done at "/permissions"
| username="alice"
| password="wonderland"
expected result = 200
actual restult = {status_code}
==>  {test_status}
'''
# query status
status_code = r.status_code
# display the results
if status_code == 200:
    test_status = 'SUCCESS'
else:
    test_status = 'FAILURE'
output = output.format(status_code=status_code, test_status=test_status)
print(output)
# printing in a file
if int(os.environ.get('LOG', 0)) == 1:
    with open('/test_results/api_test.log', 'a') as file:
        file.write(output)

# request bob user
r = requests.get(
    url='http://{address}:{port}/permissions'.format(address=api_address, port=api_port),
    params= {
        'username': 'bob',
        'password': 'builder'
    }
)
output = '''
============================
    Authentication test
============================
request done at "/permissions"
| username="bob"
| password="builder"
expected result = 200
actual restult = {status_code}
==>  {test_status}
'''
# query status
status_code = r.status_code
# display the results
if status_code == 200:
    test_status = 'SUCCESS'
else:
    test_status = 'FAILURE'
output = output.format(status_code=status_code, test_status=test_status)
print(output)
# printing in a file
if int(os.environ.get('LOG', 0)) == 1:
    with open('/test_results/api_test.log', 'a') as file:
        file.write(output)

# request clementine not a user
r = requests.get(
    url='http://{address}:{port}/permissions'.format(address=api_address, port=api_port),
    params= {
        'username': 'clementine',
        'password': 'mandarine'
    }
)
output = '''
============================
    Authentication test
============================
request done at "/permissions"
| username="clementine"
| password="mandarine"
expected result = 403
actual restult = {status_code}
==>  {test_status}
'''
# query status
status_code = r.status_code
# display the results
if status_code == 403:
    test_status = 'SUCCESS'
else:
    test_status = 'FAILURE'
output = output.format(status_code=status_code, test_status=test_status)
print(output)
# printing in a file
if int(os.environ.get('LOG', 0)) == 1:
    with open('/test_results/api_test.log', 'a') as file:
        file.write(output)
