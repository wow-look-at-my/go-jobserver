//go:build cosmo || linux

package jobserver

import (
	"syscall"
	"unsafe"
)

const affinityMaskWords = 16

// setAffinitySyscall restricts a process to the given CPUs with
// sched_setaffinity(2).
func setAffinitySyscall(pid int, cpus []int) error {
	var mask [affinityMaskWords]uintptr
	for _, c := range cpus {
		if c < 0 || c >= affinityMaskWords*64 {
			return syscall.EINVAL
		}
		mask[c/64] |= 1 << uint(c%64)
	}
	return affinityCall(pid, &mask)
}

// clearAffinitySyscall gives a process every CPU the host has.
func clearAffinitySyscall(pid, cpus int) error {
	var mask [affinityMaskWords]uintptr
	if cpus <= 0 {
		cpus = 1
	}
	if cpus > affinityMaskWords*64 {
		cpus = affinityMaskWords * 64
	}
	for c := 0; c < cpus; c++ {
		mask[c/64] |= 1 << uint(c%64)
	}
	return affinityCall(pid, &mask)
}

// getAffinitySyscall reads a process's CPU set with sched_getaffinity(2).
func getAffinitySyscall(pid int) ([]int, error) {
	var mask [affinityMaskWords]uintptr
	_, _, errno := syscall.Syscall(syscall.SYS_SCHED_GETAFFINITY, uintptr(pid),
		unsafe.Sizeof(mask), uintptr(unsafe.Pointer(&mask)))
	if errno != 0 {
		return nil, errno
	}
	var out []int
	for word, bits := range mask {
		for bit := 0; bit < 64; bit++ {
			if bits&(1<<uint(bit)) != 0 {
				out = append(out, word*64+bit)
			}
		}
	}
	return out, nil
}

// affinityCall passes a full-width CPU mask to the kernel.
func affinityCall(pid int, mask *[affinityMaskWords]uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_SCHED_SETAFFINITY, uintptr(pid),
		unsafe.Sizeof(*mask), uintptr(unsafe.Pointer(mask)))
	if errno != 0 {
		return errno
	}
	return nil
}
