
#!/usr/bin/env python
import random
def main():
	choice_set = ['matt', 'kit', 'wes', 'japhy', 'jesse', 'mark', 'rayid', 'john']
	order = []
	while len(choice_set) > 0:
		cur = random.choice(choice_set)
		choice_set.remove(cur)
		order.append(cur)
	return order

if __name__ == '__main__':
	print main()
