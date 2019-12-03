
from firebase import firebase

authentication = firebase.FirebaseAuthentication('XXX', 'XXXX@test.com', extra={'id': 'XXX'})
firebase = firebase.FirebaseApplication('https://xxxx.firebaseio.com', authentication)

result = firebase.get('/xxx', None, params={'print': 'pretty'})

