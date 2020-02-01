waterd = set()
plantd = set()
count = 0;
with open('task_execute_mageWater20190129.debug', 'r') as f1:
    line = f1.readline()
    while line is not None and line != '':
        count = count+1
        if (len(line)<5):
                line = f1.readline()
                continue
        #print(line.split(',')[0].strip())
        waterd.add(line.split(',')[0].strip())
        if (count % 10000 == 0): print(count)
        line = f1.readline()
with open('task_execute_magePlant20190129.debug', 'r') as f2:
    count =count+1
    if (count % 10000 == 0): print(count)
    line = f2.readline()
    while line is not None and line != '':
        if (len(line)<5):
                line = f2.readline()
                continue
        #print(line.split(',')[0].strip())
        plantd.add(line.split(',')[0].strip())
        line = f2.readline()
c = waterd & plantd
print (len(c))
for i in c: print(i)