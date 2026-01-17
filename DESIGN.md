
## Core Functionality

- split messages into frames
- transfer frames in stream
- stream managements

## Core Structure

### Multiplexer 
- manage stream
- send and recieve frames
- extra featrues implemented by extending Multiplexer

### Client/Server 
-- act as data sender and reciever to upper layer 
-- send and recieve data in frames
-- split data into frames then send
-- collect frames into original data
-- constrol stream via Multiplexer
