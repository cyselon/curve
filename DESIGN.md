
## Core Functionality

- split messages into frames
- transfer frames in stream
- stream managements    

## Modules

|Module|Role|Brief Description|
|---|---|---|
|Framer|The Translator|A stateless module that converts between raw bytes and Frame objects. It handles the binary protocol specification.|
|Stream|The Pipe|Implements io.ReadWriteCloser. It is the handle provided to Session layer to send/receive data for a specific ID.|
|Connection|The Engine|Manages the physical net.Conn. It handles the stream lifecycle, read/write loops, and dispatches frames to the correct Stream|
|Sesssion| The interface| Virtual Connection to the server
|SessionManager|The real connector| reponsible for how a session connect to server
|Client/Server|The Entry Point|High-level managers. Client dials a connection and returns a Session; Server listens and accepts new Sessions and Streams.|

### Interaction & Component Design
Interaction Flow
- __Physical Level:__ Data arrives at net.Conn.

- __Connection Level:__ The readLoop picks up bytes, asks Framer to parse them into a Frame.

- __Dispatch Level:__ Session Manager looks up the StreamID in its map and pushes the payload into the Session's internal channel.

- __App Level:__ User calls Session.Read(), which consumes data from that channel.


### Component Pseudo-Code (Go Style

#### Framer 

- send and recieve frames


#### Connection
- Act like a normal connection 
- A component to help client/server to manage normal tcp connections
- Act as normal Reader/Writer via Multiplexer
- constrol stream via Framer
- send and recieve data in frames
- split data into frames then send
- collect frames into original data
- multiple active streams are allowed, and limited by max_concurrent_streams
- stream 0 is reserved for control stream
- odd streams from client to server
- even streams are streams from server to client

### Stream
- 

### Client/Server 
- Connection mananger 
- components between app layer and connection

