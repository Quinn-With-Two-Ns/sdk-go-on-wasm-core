package wasmhost

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"time"

	corewasm "github.com/temporalio/sdk-go-on-wasm-core/internal/corewasm"
)

const (
	wasiSuccess = int32(0)
	wasiBadFD   = int32(8)
	wasiFault   = int32(21)
	wasiNoSys   = int32(52)
)

type WASIHost struct {
	module         *corewasm.Module
	monotonicEpoch time.Time
}

func NewWASIHost() *WASIHost {
	return &WASIHost{monotonicEpoch: time.Now()}
}

func (w *WASIHost) Init(module any) {
	w.module = module.(*corewasm.Module)
}

func (w *WASIHost) Xrandom_get(ptr, length int32) int32 {
	buffer, ok := w.memory(ptr, length)
	if !ok {
		return wasiFault
	}
	if _, err := rand.Read(buffer); err != nil {
		return wasiNoSys
	}
	return wasiSuccess
}

func (w *WASIHost) Xenviron_get(_, _ int32) int32 { return wasiSuccess }

func (w *WASIHost) Xenviron_sizes_get(countPtr, sizePtr int32) int32 {
	if !w.putU32(countPtr, 0) || !w.putU32(sizePtr, 0) {
		return wasiFault
	}
	return wasiSuccess
}

func (w *WASIHost) Xclock_time_get(clockID int32, _ int64, resultPtr int32) int32 {
	var nanos uint64
	if clockID == 1 {
		nanos = uint64(time.Since(w.monotonicEpoch).Nanoseconds())
	} else {
		nanos = uint64(time.Now().UnixNano())
	}
	if !w.putU64(resultPtr, nanos) {
		return wasiFault
	}
	return wasiSuccess
}

func (w *WASIHost) Xfd_close(_ int32) int32 { return wasiBadFD }

func (w *WASIHost) Xfd_fdstat_get(fd, statPtr int32) int32 {
	if fd < 0 || fd > 2 {
		return wasiBadFD
	}
	stat, ok := w.memory(statPtr, 24)
	if !ok {
		return wasiFault
	}
	clear(stat)
	stat[0] = 2
	return wasiSuccess
}

func (w *WASIHost) Xfd_filestat_get(_ int32, _ int32) int32  { return wasiBadFD }
func (w *WASIHost) Xfd_prestat_get(_ int32, _ int32) int32   { return wasiBadFD }
func (w *WASIHost) Xfd_prestat_dir_name(_, _, _ int32) int32 { return wasiBadFD }

func (w *WASIHost) Xfd_read(fd, _, _, bytesReadPtr int32) int32 {
	if fd != 0 {
		return wasiBadFD
	}
	if !w.putU32(bytesReadPtr, 0) {
		return wasiFault
	}
	return wasiSuccess
}

func (w *WASIHost) Xfd_write(fd, iovsPtr, iovsLen, bytesWrittenPtr int32) int32 {
	var target *os.File
	switch fd {
	case 1:
		target = os.Stdout
	case 2:
		target = os.Stderr
	default:
		return wasiBadFD
	}
	var written uint32
	for i := int32(0); i < iovsLen; i++ {
		iovec, ok := w.memory(iovsPtr+i*8, 8)
		if !ok {
			return wasiFault
		}
		ptr := int32(binary.LittleEndian.Uint32(iovec[0:4]))
		length := int32(binary.LittleEndian.Uint32(iovec[4:8]))
		data, ok := w.memory(ptr, length)
		if !ok {
			return wasiFault
		}
		n, _ := target.Write(data)
		written += uint32(n)
	}
	if !w.putU32(bytesWrittenPtr, written) {
		return wasiFault
	}
	return wasiSuccess
}

func (w *WASIHost) Xpath_open(_ int32, _ int32, _ int32, _ int32, _ int32, _ int64, _ int64, _ int32, _ int32) int32 {
	return wasiBadFD
}

func (w *WASIHost) Xpoll_oneoff(subscriptionsPtr, eventsPtr, count, eventsCountPtr int32) int32 {
	if count <= 0 {
		return wasiNoSys
	}
	var wait time.Duration
	var userdata uint64
	for i := int32(0); i < count; i++ {
		subscription, ok := w.memory(subscriptionsPtr+i*48, 48)
		if !ok {
			return wasiFault
		}
		if subscription[8] != 0 {
			continue
		}
		userdata = binary.LittleEndian.Uint64(subscription[0:8])
		timeout := binary.LittleEndian.Uint64(subscription[24:32])
		flags := binary.LittleEndian.Uint16(subscription[40:42])
		candidate := time.Duration(timeout)
		if flags&1 != 0 {
			clockID := binary.LittleEndian.Uint32(subscription[16:20])
			var now uint64
			if clockID == 1 {
				now = uint64(time.Since(w.monotonicEpoch).Nanoseconds())
			} else {
				now = uint64(time.Now().UnixNano())
			}
			if timeout <= now {
				candidate = 0
			} else {
				candidate = time.Duration(timeout - now)
			}
		}
		if wait == 0 || candidate < wait {
			wait = candidate
		}
	}
	if wait > 0 {
		time.Sleep(wait)
	}
	event, ok := w.memory(eventsPtr, 32)
	if !ok {
		return wasiFault
	}
	clear(event)
	binary.LittleEndian.PutUint64(event[0:8], userdata)
	if !w.putU32(eventsCountPtr, 1) {
		return wasiFault
	}
	return wasiSuccess
}

func (w *WASIHost) Xproc_exit(code int32) {
	panic(fmt.Sprintf("WASI process exited with code %d", code))
}

func (w *WASIHost) Xsched_yield() int32 {
	runtime.Gosched()
	return wasiSuccess
}

func (w *WASIHost) memory(ptr, length int32) ([]byte, bool) {
	if ptr < 0 || length < 0 {
		return nil, false
	}
	memory := *w.module.Xmemory().Slice()
	start := uint64(uint32(ptr))
	end := start + uint64(uint32(length))
	if end > uint64(len(memory)) {
		return nil, false
	}
	return memory[start:end], true
}

func (w *WASIHost) putU32(ptr int32, value uint32) bool {
	buffer, ok := w.memory(ptr, 4)
	if ok {
		binary.LittleEndian.PutUint32(buffer, value)
	}
	return ok
}

func (w *WASIHost) putU64(ptr int32, value uint64) bool {
	buffer, ok := w.memory(ptr, 8)
	if ok {
		binary.LittleEndian.PutUint64(buffer, value)
	}
	return ok
}
