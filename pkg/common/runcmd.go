package common

import (
	"fmt"
	"os/exec"
	"strings"

	"k8s.io/klog/v2"
)

const (
	// Error thrown by exec cmd.Run() when process spawned by cmd.Start() completes before cmd.Wait() is called (see - k/k issue #103753)
	errNoChildProcesses = "wait: no child processes"
)

// RunCommand wraps a k8s exec to deal with the no child process error. Same as exec.CombinedOutput.
// On error, the output is included so callers don't need to echo it again.
func RunCommand(cmd string, args ...string) ([]byte, error) {
	klog.V(2).Infof("====== Start RunCommand ======")
	execCmd := exec.Command(cmd, args...)
	output, err := execCmd.CombinedOutput()
	klog.V(2).Infof("====== RunCommand output: %v ======", string(output))
	if err != nil {
		if err.Error() == errNoChildProcesses {
			if execCmd.ProcessState.Success() {
				// If the process succeeded, this can be ignored, see k/k issue #103753
				return output, nil
			}
			// Get actual error
			err = &exec.ExitError{ProcessState: execCmd.ProcessState}
		}
		klog.V(2).Infof("====== RunCommand error is: %v ======", err)
		return output, fmt.Errorf("%s %s failed: %w; output: %s", cmd, strings.Join(args, " "), err, string(output))
	}
	return output, nil
}
