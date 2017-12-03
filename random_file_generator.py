from datetime import datetime
import random 
import time

n = random.randint(1, 1000)
delay = 3
max_files = 1000
print("This utility continuously create new file that contains random numbers")
i = 0
while i < max_files:
  lines = [str(random.random()) + "\n" for _ in range(n)]
  file_name = datetime.now().strftime("%s") + ".dat"
  with open(file_name, "w") as f:
      f.writelines(lines)
      print(file_name)
  time.sleep(delay)
  i = i + 1
 
