package task

var stateTransitions = map[State][]State{
	Pending:   {Scheduled},
	Scheduled: {Scheduled, Running, Failed},
	Running:   {Running, Completed, Failed},
	Completed: {},
	Failed:    {},
}

func Contains(states []State, state State) bool {
	for _, v := range states {
		if v == state {
			return true
		}
	}
	return false
}

func ValidateTransitions(src State, dst State) bool {
	return Contains(stateTransitions[src], dst)
}
