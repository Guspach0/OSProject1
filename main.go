package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

type ProcessStatus struct {
	ProcessID int64
	StartTime int64
	EndTime   int64
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	processesBurst := make([]Process, len(processes))
	copy(processesBurst, processes)
	// initializing variables
	var (
		schedule            = make([][]string, len(processes))
		gantt               = make([]TimeSlice, 0)
		currentTime         int64
		totalWaitTime       int64
		totalTurnaroundTime int64
		numCompleted        int64
	)
	//making a copy to use to get burst duration later since I subtract from the orginal burst duration
	processesCopy := make([]Process, len(processes))
	copy(processesCopy, processes)

	// sort both arrays by arrival time
	sort.Slice(processesBurst, func(i, j int) bool {
		return processesBurst[i].ArrivalTime < processesBurst[j].ArrivalTime
	})
	sort.Slice(processesCopy, func(i, j int) bool {
		return processesCopy[i].ArrivalTime < processesCopy[j].ArrivalTime
	})

	// n is the number of process and status is meant to store the id and start/end times.
	n := len(processes)
	status := make([]ProcessStatus, n)

	// while I have not completed all the process, go to the next "second"
	for numCompleted < int64(n) {
		var shortestJobIndex int = -1
		var shortestJobDuration int64 = 999 //not sure how long the burst can be, but I doubt it will be bigger than 999

		// get the shortest process that hasnt had an end time
		for i := 0; i < n; i++ {
			if processesBurst[i].ArrivalTime <= currentTime && status[i].EndTime == 0 && processesBurst[i].BurstDuration < shortestJobDuration {
				shortestJobIndex = i
				shortestJobDuration = processesBurst[i].BurstDuration
			}
		}

		// no job, go to next "second"
		if shortestJobIndex == -1 {
			currentTime++
			continue
		}

		// if we have a process, get the start time and subtract a second
		status[shortestJobIndex].ProcessID = processesBurst[shortestJobIndex].ProcessID
		if status[shortestJobIndex].StartTime == 0 {
			status[shortestJobIndex].StartTime = currentTime
		}
		processesBurst[shortestJobIndex].BurstDuration--
		currentTime++

		// if the process is done, get the end time, add it to completed tally, and get wait and turnaround
		if processesBurst[shortestJobIndex].BurstDuration == 0 {
			status[shortestJobIndex].EndTime = currentTime
			numCompleted++
			if status[shortestJobIndex].StartTime == 1 {
				status[shortestJobIndex].StartTime = 0
			}
			totalWaitTime += status[shortestJobIndex].StartTime - processesBurst[shortestJobIndex].ArrivalTime
			totalTurnaroundTime += status[shortestJobIndex].EndTime - processesBurst[shortestJobIndex].ArrivalTime
		}
	}

	// calculate and output, formatted into the original style
	averageWaitTime := float64(totalWaitTime) / float64(n)
	averageTurnaroundTime := float64(totalTurnaroundTime) / float64(n)
	throughput := float64(n) / float64(currentTime)

	for i := 0; i < n; i++ {
		waitTime := status[i].StartTime - processesBurst[i].ArrivalTime
		turnaroundTime := status[i].EndTime - processesBurst[i].ArrivalTime
		schedule[i] = []string{
			fmt.Sprint(processesBurst[i].ProcessID),
			fmt.Sprint(processesBurst[i].Priority),
			fmt.Sprint(processesCopy[i].BurstDuration),
			fmt.Sprint(processesBurst[i].ArrivalTime),
			fmt.Sprint(waitTime),
			fmt.Sprint(turnaroundTime),
			fmt.Sprint(status[i].EndTime),
		}
		gantt = append(gantt, TimeSlice{
			PID:   processesBurst[i].ProcessID,
			Start: status[i].StartTime,
			Stop:  turnaroundTime,
		})

	}
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, averageWaitTime, averageTurnaroundTime, throughput)
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	// initialize variables
	var (
		currentTime         int64
		totalWaitTime       int64
		totalTurnaroundTime int64
		schedule            = make([][]string, len(processes))
		gantt               = make([]TimeSlice, 0)
	)
	// make copies of the processes
	processesBurst := make([]Process, len(processes))
	copy(processesBurst, processes)
	processesCopy := make([]Process, len(processes))
	copy(processesCopy, processes)

	// variable to get the number of processes and to track the start/end times
	n := len(processes)
	status := make([]ProcessStatus, n)

	// sort
	sort.Slice(processesBurst, func(i, j int) bool {
		return processesBurst[i].ArrivalTime < processesBurst[j].ArrivalTime
	})
	sort.Slice(processesCopy, func(i, j int) bool {
		return processesCopy[i].ArrivalTime < processesCopy[j].ArrivalTime
	})

	// while there are still jobs unfinished
	for numCompleted := 0; numCompleted < n; currentTime++ {
		var shortestJobIndex int = -1
		var shortestJobDuration int64 = 1<<63 - 1
		//find the shortest job that isnt done
		for i := 0; i < n; i++ {
			if processesBurst[i].ArrivalTime <= currentTime && status[i].EndTime == 0 && processesBurst[i].BurstDuration < shortestJobDuration {
				shortestJobIndex = i
				shortestJobDuration = processesBurst[i].BurstDuration
			}
		}

		// when we are at a process that isnt done, take note of the time started, subtract the burst duration, and check if done while
		// storing the times for the output
		if shortestJobIndex != -1 {
			status[shortestJobIndex].StartTime = currentTime
			processesBurst[shortestJobIndex].BurstDuration--
			if processesBurst[shortestJobIndex].BurstDuration == 0 {
				numCompleted++
				status[shortestJobIndex].EndTime = currentTime + 1
				totalWaitTime += status[shortestJobIndex].StartTime - processesBurst[shortestJobIndex].ArrivalTime
				totalTurnaroundTime += status[shortestJobIndex].EndTime - processesBurst[shortestJobIndex].ArrivalTime
			}
		}
	}

	// calculate and output, formatted into the original style
	for i := 0; i < n; i++ {
		waitTime := status[i].StartTime - processesBurst[i].ArrivalTime
		turnaroundTime := status[i].EndTime - processesBurst[i].ArrivalTime

		schedule[i] = []string{
			fmt.Sprint(processesBurst[i].ProcessID),
			fmt.Sprint(processesBurst[i].Priority),
			fmt.Sprint(processesCopy[i].BurstDuration),
			fmt.Sprint(processesBurst[i].ArrivalTime),
			fmt.Sprint(waitTime),
			fmt.Sprint(turnaroundTime),
			fmt.Sprint(status[i].EndTime),
		}
		gantt = append(gantt, TimeSlice{
			PID:   processesBurst[i].ProcessID,
			Start: status[i].StartTime,
			Stop:  turnaroundTime,
		})
	}

	averageWaitTime := float64(totalWaitTime) / float64(n)
	averageTurnaroundTime := float64(totalTurnaroundTime) / float64(n)
	throughput := float64(n) / float64(currentTime)

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, averageWaitTime, averageTurnaroundTime, throughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {
	// initializing variables
	var (
		currentTime         int64
		totalWaitTime       int64
		totalTurnaroundTime int64
		numCompleted        int64
		schedule            = make([][]string, len(processes))
		gantt               = make([]TimeSlice, 0)
	)
	// make copies of the processes
	processesBurst := make([]Process, len(processes))
	copy(processesBurst, processes)
	processesCopy := make([]Process, len(processes))
	copy(processesCopy, processes)

	// variable to get the number of processes and to track the start/end times
	n := len(processes)
	status := make([]ProcessStatus, n)

	// initialize the queue with the indices of the processes
	queue := make([]int, n)
	for i := 0; i < n; i++ {
		queue[i] = i
	}

	for numCompleted < int64(n) {
		// get the index of the next process to execute
		index := queue[0]
		queue = queue[1:]

		if status[index].StartTime == 0 {
			status[index].StartTime = currentTime
		}

		// subtract the burst duration by a time quantum of 1
		if processes[index].BurstDuration > 1 {
			processes[index].BurstDuration--
			queue = append(queue, index)
		} else {
			processes[index].BurstDuration = 0
			status[index].EndTime = currentTime + 1
			numCompleted++
			totalTurnaroundTime += status[index].EndTime - processes[index].ArrivalTime
		}

		currentTime++
	}

	// calculate and output, formatted into the original style
	for i := 0; i < n; i++ {
		turnaroundTime := status[i].EndTime - processesBurst[i].ArrivalTime
		totalWaitTime += turnaroundTime - processesCopy[i].BurstDuration
	}
	averageWaitTime := float64(totalWaitTime) / float64(n)
	averageTurnaroundTime := float64(totalTurnaroundTime) / float64(n)
	throughput := float64(n) / float64(currentTime)

	//
	for i := 0; i < n; i++ {

		turnaroundTime := status[i].EndTime - processesBurst[i].ArrivalTime
		waitTime := turnaroundTime - processesCopy[i].BurstDuration
		schedule[i] = []string{
			fmt.Sprint(processesBurst[i].ProcessID),
			fmt.Sprint(processesBurst[i].Priority),
			fmt.Sprint(processesCopy[i].BurstDuration),
			fmt.Sprint(processesBurst[i].ArrivalTime),
			fmt.Sprint(waitTime),
			fmt.Sprint(turnaroundTime),
			fmt.Sprint(status[i].EndTime),
		}
		gantt = append(gantt, TimeSlice{
			PID:   processesBurst[i].ProcessID,
			Start: status[i].StartTime,
			Stop:  turnaroundTime,
		})
	}
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, averageWaitTime, averageTurnaroundTime, throughput)
}

func copyRemainingTime(dst []int64, src []Process) {
	for i := range src {
		dst[i] = src[i].BurstDuration
	}
}

func allCompleted(completed []bool) bool {
	for i := range completed {
		if !completed[i] {
			return false
		}
	}
	return true
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
