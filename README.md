# aws_persistent_pubsub
Persistent PubSub on AWS infrastructure.

## Example

Create a listener
```python
from aws_persistent_pubsub import PubSub

app = PubSub("SomeListener", region_name='eu-west-1')

# register event listener
@app.listen("User", ["Entered", "Left"])
def user_created(source, event, target, trigger_datetime):
    user = json.loads(target)
    if event == "Entered":
        send_email(user['email'], "Welcome to {}!".format(source))
    elif event == "Left":
        send_email(user['email'], "Cya!")
  
# create sqs queue, sns topics and link them
app.register_aws_resources()

# wait for messages
while True:
    app.poll()
```

Create a source application
```python
from aws_persistent_pubsub import PubSub

app = PubSub("#channel", region_name='eu-west-1')

# no need to register aws resource because we don't want to receive messages
app.emit("User", "Entered", json.dumps(email="email@example.com"))
```
