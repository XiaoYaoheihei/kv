package memtable

type Stack struct {
	stack []*treeNode
	//栈底索引
	base int
	//栈顶索引
	top int
}

// 初始化栈
func Initstack(n int) Stack {
	stack := Stack{
		stack: make([]*treeNode, n),
		base:  0,
		top:   0,
	}
	return stack
}

func (s *Stack) Push(v *treeNode) {
	//栈满
	if s.top == len(s.stack) {
		s.stack = append(s.stack, v)
	} else {
		s.stack[s.top] = v
	}
	s.top++
}

func (s *Stack) Pop() (*treeNode, bool) {
	//空栈情况
	if s.top == s.base {
		return nil, false
	}
	s.top--
	return s.stack[s.top], true
}
