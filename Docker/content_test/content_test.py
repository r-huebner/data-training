import os
import requests

# definition of the API address
api_address = os.environ.get('API_ADR')

# API port
api_port = 8000

# request alice v1 positive
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
    Content test
============================
request done at "/v1/sentiment"
| username="alice"
| password="wonderland"
| sentence="life is beautiful"
expected result = positive
actual restult = {status_code}
score = {score}
==>  {test_status}
'''
# sentence score
data=r.json()
score=data["score"]

# display the results
if score >=0:
    status_code = "positive"
    test_status = 'SUCCESS'
else:
    status_code = "negative"
    test_status = 'FAILURE'
output = output.format(status_code=status_code, test_status=test_status, score=score)
print(output)
# printing in a file
if int(os.environ.get('LOG', 0)) == 1:
    with open('/test_results/api_test.log', 'a') as file:
        file.write(output)

# request alice v1 negative
r = requests.get(
    url='http://{address}:{port}/v1/sentiment'.format(address=api_address, port=api_port),
    params= {
        'username': 'alice',
        'password': 'wonderland',
        'sentence': 'that sucks'
    }
)
output = '''
============================
    Content test
============================
request done at "/v1/sentiment"
| username="alice"
| password="wonderland"
| sentence="that sucks"
expected result = negative
actual restult = {status_code}
score = {score}
==>  {test_status}
'''
# sentence score
data=r.json()
score=data["score"]

# display the results
if score >=0:
    status_code = "positive"
    test_status = 'FAILURE'
else:
    status_code = "negative"
    test_status = 'SUCCESS'

output = output.format(status_code=status_code, test_status=test_status, score=score)
print(output)
# printing in a file
if int(os.environ.get('LOG', 0)) == 1:
    with open('/test_results/api_test.log', 'a') as file:
        file.write(output)

# request alice v2 positive
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
    Content test
============================
request done at "/v2/sentiment"
| username="alice"
| password="wonderland"
| sentence="life is beautiful"
expected result = positive
actual restult = {status_code}
score = {score}
==>  {test_status}
'''
# sentence score
data=r.json()
score=data["score"]

# display the results
if score >=0:
    status_code = "positive"
    test_status = 'SUCCESS'
else:
    status_code = "negative"
    test_status = 'FAILURE'
output = output.format(status_code=status_code, test_status=test_status, score=score)
print(output)
# printing in a file
if int(os.environ.get('LOG', 0)) == 1:
    with open('/test_results/api_test.log', 'a') as file:
        file.write(output)

# request alice v2 negative
r = requests.get(
    url='http://{address}:{port}/v2/sentiment'.format(address=api_address, port=api_port),
    params= {
        'username': 'alice',
        'password': 'wonderland',
        'sentence': 'that sucks'
    }
)
output = '''
============================
    Content test
============================
request done at "/v2/sentiment"
| username="alice"
| password="wonderland"
| sentence="that sucks"
expected result = negative
actual restult = {status_code}
score = {score}
==>  {test_status}
'''
# sentence score
data=r.json()
score=data["score"]

# display the results
if score >=0:
    status_code = "positive"
    test_status = 'FAILURE'
else:
    status_code = "negative"
    test_status = 'SUCCESS'

output = output.format(status_code=status_code, test_status=test_status, score=score)
print(output)
# printing in a file
if int(os.environ.get('LOG', 0)) == 1:
    with open('/test_results/api_test.log', 'a') as file:
        file.write(output)

