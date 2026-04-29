// An example script showing usage of FUSE file descriptor as a mount point.
// The mount helper keeps the FUSE fd open and relaunches Mountpoint whenever it stops,
// preserving the mount across Mountpoint restarts.
//
// Example usage:
// $ go build ./examples/fuse-fd-mount-point/mounthelper.go
// $ sudo /sbin/setcap 'cap_sys_admin=ep' ./mounthelper # `mount` syscall requires `CAP_SYS_ADMIN`, alternatively, `mounthelper` can be run as root
// $ ./mounthelper -mountpoint /tmp/mountpoint -bucket bucketname
// $ # Mountpoint mounted at /tmp/mountpoint until `mounthelper` is terminated with ctrl+c.
// $ # If Mountpoint exits, it will be automatically relaunched using the same FUSE fd.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
)

var mountPoint = flag.String("mountpoint", "", "Path to mount the filesystem at")
var bucket = flag.String("bucket", "", "S3 Bucket to mount")

func main() {
	flag.Parse()

	// `os.MkdirAll` will return `nil` if `mountPoint` is an already existing directory.
	if err := os.MkdirAll(*mountPoint, 0644); err != nil {
		log.Panicf("Failed to create target mount point %s: %v\n", *mountPoint, err)
	}

	// 1. Open FUSE device
	const USE_DEFAULT_PERM = 0
	fd, err := syscall.Open("/dev/fuse", os.O_RDWR, USE_DEFAULT_PERM)
	if err != nil {
		log.Panicf("Failed to open /dev/fuse: %v\n", err)
	}

	// Close the fd when the helper exits.
	defer func() {
		err := syscall.Close(fd)
		if err != nil {
			log.Printf("Failed to close fd: %v\n", err)
		}
	}()

	var stat syscall.Stat_t
	err = syscall.Stat(*mountPoint, &stat)
	if err != nil {
		log.Panicf("Failed to stat mount point %s: %v\n", *mountPoint, err)
	}

	// 2. Perform `mount` syscall
	// These mount options and flags match those typically set when using Mountpoint.
	// Some are set by the underlying FUSE library.
	// Mountpoint sets (correct at the time of authoring this comment):
	// * `noatime` to avoid unsupported access time updates.
	// * `default_permissions` to tell the Kernel to evaluate permissions itself, since Mountpoint does not currently provide any handler for FUSE `access`.
	options := []string{
		fmt.Sprintf("fd=%d", fd),
		fmt.Sprintf("rootmode=%o", stat.Mode),
		fmt.Sprintf("user_id=%d", os.Geteuid()),
		fmt.Sprintf("group_id=%d", os.Getegid()),
		"default_permissions",
	}
	var flags uintptr = syscall.MS_NOSUID | syscall.MS_NODEV | syscall.MS_NOATIME
	err = syscall.Mount("mountpoint-s3", *mountPoint, "fuse", flags, strings.Join(options, ","))
	if err != nil {
		log.Panicf("Failed to call mount syscall: %v\n", err)
	}

	// 3. Define and defer call to `unmount` syscall, to be invoked once the helper terminates
	defer func() {
		err := syscall.Unmount(*mountPoint, 0)
		if err != nil {
			log.Printf("Failed to unmount %s: %v\n", *mountPoint, err)
		} else {
			log.Printf("Successfully unmounted %s\n", *mountPoint)
		}
	}()

	// 4. Listen for termination signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// 5. Launch Mountpoint in a loop, restarting it whenever it exits.
	//    The FUSE fd stays open in this process, so the mount remains valid across restarts.
	for {
		mountpointCmd := exec.Command("./target/release/mount-s3",
			*bucket,
			fmt.Sprintf("/dev/fd/%d", fd),
			// Other mount options can be added here
			"--foreground",
			"--allow-delete",
			// Enable verbose logs for debugging
			// "--debug",
			// "--debug-crt",
		)
		mountpointCmd.Stdout = os.Stdout
		mountpointCmd.Stderr = os.Stderr
		err = mountpointCmd.Start()
		if err != nil {
			log.Panicf("Failed to start Mountpoint: %v\n", err)
		}

		// Wait for either Mountpoint to exit or a termination signal.
		exited := make(chan error, 1)
		go func() {
			exited <- mountpointCmd.Wait()
		}()

		select {
		case <-stop:
			// Helper received a signal — kill Mountpoint and exit.
			log.Print("Received termination signal, shutting down")
			mountpointCmd.Process.Signal(syscall.SIGTERM)
			<-exited
			// Deferred unmount and fd close will run on return.
			return
		case err := <-exited:
			// Mountpoint exited on its own — relaunch it.
			log.Printf("Mountpoint exited: %v. Relaunching...\n", err)
		}
	}
}
