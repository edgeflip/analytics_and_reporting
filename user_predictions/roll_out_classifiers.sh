script_dir='~/analytics_and_reporting/user_predictions'
log_dir='~/analytics_and_reporting/user_predictions/logs'

echo environment
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py environment 100000 >> ${log_dir}/batch_environment.log 2>&1 &

echo asian
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py asian_100k 100000 >> ${log_dir}/batch_asian_100k.log 2>&1 &

echo veteran
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py veteran nonveterans >> ${log_dir}/batch_veteran.log 2>&1 &

echo hispanic
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py hispanic_100k 100000 >> ${log_dir}/batch_hispanic.log 2>&1 &

echo african american
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py african_american_100k 100000 >> ${log_dir}/batch_african_american.log 2>&1 &

echo lgbt
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py lgbt 100000 >> ${log_dir}/batch_lgbt.log 2>&1 &

echo parent
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py parent nonparents_100k >> ${log_dir}/batch_parent.log 2>&1 &

echo vegan
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py vegan None >> ${log_dir}/batch_vegan.log 2>&1 &

echo female
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py female male >> ${log_dir}/batch_female.log 2>&1 &

echo liberal
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py liberal conservative >> ${log_dir}/batch_liberal.log 2>&1 &

echo christian
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py christian nonchristians >> ${log_dir}/batch_christian.log 2>&1 &

echo jewish
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py jewish 100000 >> ${log_dir}/batch_jewish.log 2>&1 &

echo muslim
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py muslim 100000 >> ${log_dir}/batch_muslim.log 2>&1 &

echo atheist
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py atheist 100000 >> ${log_dir}/batch_atheist.log 2>&1 &
