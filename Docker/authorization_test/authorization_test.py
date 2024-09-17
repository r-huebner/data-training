import os
import requests

# definition of the API address
api_address = os.environ.get('API_ADR')

# API port
api_port = 8000

# request alice v1, v2 access (v1)
r = requests.get(
    url='http://{address}:{port}/v1/sentiment'.format(address=api_address, port=api_port),
    params= {
        'username': 'alice',
        'password': 'wonderland',
        'sentence': 'life is beautiful'
    }
)
output = '''
============================
    Authorization test
============================
request done at "/v1/sentiment"
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
print(output.format(status_code=status_code, test_status=test_status))
# printing in a file
if os.environ.get('LOG') == 1:
    with open('api_test.log', 'a') as file:
        file.write(output)

# request alice v1, v2 access (v2)
r = requests.get(
    url='http://{address}:{port}/v2/sentiment'.format(address=api_address, port=api_port),
    params= {
        'username': 'alice',
        'password': 'wonderland',
        'sentence': 'life is beautiful'
    }
)
output = '''
============================
    Authorization test
============================
request done at "/v2/sentiment"
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

# request bob v1 access (v1)
r = requests.get(
    url='http://{address}:{port}/v1/sentiment'.format(address=api_address, port=api_port),
    params= {
        'username': 'bob',
        'password': 'builder',
        'sentence': 'life is beautiful'
    }
)
output = '''
============================
    Authorization test
============================
request done at "/v1/sentiment"
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

# request bob v1 access (v2)
r = requests.get(
    url='http://{address}:{port}/v2/sentiment'.format(address=api_address, port=api_port),
    params= {
        'username': 'bob',
        'password': 'builder',
        'sentence': 'life is beautiful'
    }
)
output = '''
============================
    Authorization test
============================
request done at "/v2/sentiment"
| username="bob"
| password="builder"
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

