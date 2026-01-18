
## Core Functionality

- split messages into frames
- transfer frames in stream
- stream managements

## Core Structure

### Multiplexer 
- manage stream
- send and recieve frames
- extra featrues implemented by extending Multiplexer

### Connection
- Act like a normal connection 
- A component to help client/server to manage normal tcp connections
- Act as normal Reader/Writer via Multiplexer
- constrol stream via Multiplexer
- send and recieve data in frames
- split data into frames then send
- collect frames into original data
- multiple active streams are allowed, and limited by max_concurrent_streams
- stream 0 is reserved for control stream
- odd streams from client to server
- even streams are streams from server to client



### Client/Server 
- Connection mananger 
- components between app layser and connection

