# RAD - Rapid Application Development

## Overview

The concept is simple: "what if you could easily construct an application by using other applications as 
building blocks?". Simply say what applications to start and how they should communicate using a simple config file,
and your problem is mostly, or completely, solved.

## How?

The basic idea is to have a messaging system that acts as a communications bus. There are 2 options:
- Applications use a communications library to communicate. The library connects the application directly to the system
- Applications integrate using eg. stdio and a utility wrapper. The wrapper integrates with stdio, and implements the communications library.
  This makes it possible to use command like tools (eg. ifconfig, ls, sort, etc) with the system without modification

Each "application" is associated with one or more message types eg. string, int, my_custom_type. You can create whatever
types you like. The configuration file then contains how messages should be routed. Messages of one or more types may be routed to
one or more other applications. The routing engine is responsible for figuring out what messages go where.

## Why not just use a script?

Shell scripts are great, but often lack the flexibility offered by this system. This system is essentially the shell 
pipe command ("|") on steroids. This system technically lets you mix databases with message queues with web servers with
custom python/golang/rust/C++ scripts/apps.

## Where are all these apps?
There's a server. Apps can be downloaded from a repository. Think of how rust/maven/golang have dependencies that can be
auto-downloaded depending on project requirements. The idea is the same.

## What if I need my own logic?

No problem. Use the existing apps where available. Then, write your own code and either:
- use the comms library to connect to the system or
- use one of the wrapper tools to connect your app to the system (no coding required)

## Example

Here's an example configuration file:

```
#note: this config file does NOT need to be in the config directory
#it can be anywhere
server:
  addr: "http://localhost:8080"
broker:
  bind_addr: "0.0.0.0:8888"
apps:
  #format app_name (optional version) [optional instance IDs (comma separated)]
  #if instance IDs are not specified, defaults to 0 only
  - echo (0.0.1) 0,1
routing:
  #format src <types comma separated> [optional routing keys, comma separated] -> dst1[,dst2[,dst3[...]]]
  - echo-0 string -> echo-1
  - echo-1 string -> echo-0
```

This says:
1. Connect to the server running at localhost:8080 (for downloading apps, etc)
2. Offer a service on port 8888 for apps to communicate
3. There's only one app in this example called "echo". The version of the app is 0.0.1. For this
   For this workflow, we want to run 2 instances of echo (0,1). In other words, the system will start
   echo-0 and echo-1
4. echo-0 will route all string messages to echo-1. Similarly, echo-1 will route all string messages to echo-0
   thereby creating an infinite loop. It's useful for testing.

Notes:
- The echo application will be downloaded from the server. For debugging, it is possible to
  run applications locally as well, even from within your IDE
- The example above shows basic routing. More complex variations are possible


   
