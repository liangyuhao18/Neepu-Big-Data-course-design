import pandas as pd
#.csv->.txt
data = pd.read_csv('us-counties.csv')
with open('us-counties.txt', 'a+', encoding='utf-8') as f:
 for line in data.values:
  f.write((str(line[0])+'\t'+str(line[1])+'\t'
           + str(line[2]) + '\t' + str(line[3]) + '\t' + str(line[4]) + '\n'))
