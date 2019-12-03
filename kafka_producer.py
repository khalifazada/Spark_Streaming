from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from kafka import SimpleProducer, KafkaClient
import json

from twitter_auth import ACCESS_TOKEN, TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET

# Twitter Authentication
authentication = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
authentication.set_access_token(ACCESS_TOKEN, TOKEN_SECRET)

# Twitter Stream Listener
HASHTAG = "bigdata"					# track hashtag
LOCALHOST = "localhost:9092"		# connect to
TOPIC = "twitter"					# filter by topic

# create Kafka Listener
class KafkaListener(StreamListener):
    
	def on_data(self, data):
		producer.send_messages(TOPIC, data.encode('utf-8'))
		print(data)
		return True

	def on_error(self, status):
		print(status)

# define client
client = KafkaClient(LOCALHOST)

# define producer
producer = SimpleProducer(client)

# define listener
listener = KafkaListener()

# set stream & track HASHTAG
stream = Stream(authentication, listener)
stream.filter(track=HASHTAG)
