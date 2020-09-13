
from google.cloud import bigquery
from google.auth import credentials
import os

class Credentials(credentials.AnonymousCredentials):
	def __init__(self, token):
		self.token = token
	def apply(self, headers):
		headers['Authorization'] = 'Bearer {}'.format(self.token)
	def before_request(self, request, method, url, headers):
		self.apply(headers)


class Client(bigquery.Client):
	def __init__(
		self,
		api_endpoint = "https://redivis.com/api/v1"
	):
		credential = Credentials(os.getenv('REDIVIS_API_TOKEN'))
		super(Client, self).__init__(
			credentials=credential, project="_", client_options={"api_endpoint": api_endpoint}
		)
