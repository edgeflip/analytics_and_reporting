from db_utils import redshift_connect, redshift_disconnect, execute_query
import pandas as pd
import numpy as np
import sys

label_and_table_names = []
label_and_table_names.append( ('female', 'user_px5_female_updated') )
label_and_table_names.append( ('vegan', 'user_px5_vegan_updated') )
label_and_table_names.append( ('parent', 'user_px5_parent_updated') )
label_and_table_names.append( ('lgbt', 'user_px5_lgbt_updated') )
label_and_table_names.append( ('veteran', 'user_px5_veteran_updated') )
label_and_table_names.append( ('environment', 'user_px5_environment_updated') )
label_and_table_names.append( ('african american', 'user_px5_african_american_100k_updated') )
label_and_table_names.append( ('hispanic', 'user_px5_hispanic_100k_updated') )
label_and_table_names.append( ('asian', 'user_px5_asian_100k_updated') )
label_and_table_names.append( ('liberal', 'user_px5_liberal_updated') )
label_and_table_names.append( ('christian', 'user_px5_christian_updated') )
label_and_table_names.append( ('jewish', 'user_px5_jewish_updated') )

conn = redshift_connect()
labels = []
values = [] # total count, non-null count, positive count, negative count, positive proportion

sys.stdout.write('getting stats for:\n')
for label, table_name in label_and_table_names:
    sys.stdout.write('{} '.format(label))
    sys.stdout.flush()
    query = """
            SELECT total, non_null, positive, negative, positive/non_null::decimal AS proportion_positive
            FROM 
                ( SELECT COUNT(*) AS total FROM {table_name} ),
                ( SELECT COUNT(*) AS non_null FROM {table_name} WHERE prediction IS NOT NULL ), 
                ( SELECT COUNT(*) AS positive FROM {table_name} WHERE prediction > 0 ), 
                ( SELECT COUNT(*) AS negative FROM {table_name} WHERE prediction < 0 )
            """.format(table_name=table_name)
    rows = execute_query(query, conn)
    labels.append(label)
    values.append((int(rows[0][0]), int(rows[0][1]), int(rows[0][2]), int(rows[0][3]), float(rows[0][4])))
sys.stdout.write('\n')

stats = pd.DataFrame(data=np.array(values, dtype=('int32,int32,int32,int32,float64')),                      
                     index=labels)
stats.columns = ['total', 'non-null', 'positive', 'negative', 'proportion']
sys.stdout.write('{}'.format(stats))
sys.stdout.write('\n')
redshift_disconnect(conn)