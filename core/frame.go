package core

type FrameType byte

const (
    FrameData    FrameType = iota // 数据帧
    FrameControl                   // 控制帧（如窗口更新、PING）
)

type Frame struct {
    StreamID uint32
    Type     FrameType
    Flags    byte
    Payload  []byte
}
