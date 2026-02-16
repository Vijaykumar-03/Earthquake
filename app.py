import json, boto3, urllib.request
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('USGSEarthquakeEvents')
sns = boto3.client('sns')

SNS_ARN = "arn:aws:sns:ap-south-1:xxxx:Earthquake"

URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

def run():
    data = json.loads(urllib.request.urlopen(URL).read())
    latest = data["features"][0]
    latest_id = latest["id"]

    old = table.get_item(Key={"id":"last"})
    last_id = old["Item"]["event_id"] if "Item" in old else None

    if latest_id != last_id:
        msg = f"Earthquake Alert: {latest['properties']['place']}"
        sns.publish(TopicArn=SNS_ARN, Message=msg)
        table.put_item(Item={"id":"last","event_id":latest_id})

while True:
    run()
