import sys
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import pykafka
class TweetsListener(StreamListener):
   def __init__(self,kafkaProducer):
      print("-----------------Tweets producer is initialized------------")
      self.producer=kafkaProducer
   def on_data(self,data):
      try:
        json_data=json.loads(data)
        tweet=json_data['text']
        print(tweet+'\n')
        self.producer.produce(bytes(json.dumps(tweet),"utf-8"))
      except KeyError as e:
	       print("Error on data ",e)
      return True
   def on_error(self,status):
      print(status)
      return True
   def connect_to_twitter(kafkaProducer,tracks):
      api_key="zhZAQipUqh1RoJuz4APTneIaP"
      api_secret="1EfebQPCMRkwsCSLpucK15CYM6CnQCsQGxcAGbttqmziSxMKvL"
      access_token="1452157069608427524-cSwY0QrY3KCoZvs0A6Qb2cQS2OpCxU"
      access_token_secret="lxqqOonH5O1BsPIGZoH4cLzS0us5jux69m3cgWtl6OsOu"
      auth=OAuthHandler(api_key,api_secret)
      auth.set_access_token(access_token,access_token_secret)
      twitter_stream=Stream(auth,TweetsListener(kafkaProducer))
      twitter_stream.filter(track=tracks,languages=["en"])
if __name__=="__main__":
   if len(sys.argv)<4:
      print("Invalid input")
      exit(-1)
   host_port=sys.argv[1]
   topic=sys.argv[2]
   tracks=sys.argv[3:]
   kafkaClient=pykafka.KafkaClient(host_port)
   kafkaProducer=kafkaClient.topics[bytes(topic).encode('utf-8')].get_producer()
   connect_to_twitter(kafkaProducer,tracks)