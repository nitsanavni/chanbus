# Claude Code Channel Bus

## Description

Multiple claude code session register on the bus and can talk to each other
- Messages can be broadcast messages
- Message can be direct messages bz addressing by name

## Features

- spin up the bus manually
- Connect to the bus
- Sending
  - Individual msgs by name: `send "this message" to "abc"`
  - Broadcast to all (except self)
- Receiving
  - DMs: msgs addressed to that claude code
  - receive broadcasted msg
- Register your name
- Querying all instances

## main server (hub)

the central registration of claude channels
it is a cli
start with `hub`
logs operations in the terminal
### Sample log
```
    hub started
    claude instance "abc" registered in the hub
    claude instance "xyz" registered in the hub
    received message of type broadcast from instance "abc" with content:"message"
    message with content "message" sent to claude instance "abc"  
```
