script_dir='~/analytics_and_reporting/text_models'

echo environment
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py environment 100000 >> batch_environment_log.out 2>&1 &

echo asian
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py asian_100k 100000 >> batch_asian_100k_log.out 2>&1 &

echo veteran
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py veteran nonveterans >> batch_veteran_updated_log.out 2>&1 &

echo hispanic
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py hispanic_100k 100000 >> batch_hispanic_100k_log.out 2>&1 &

echo african american
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py african_american_100k 100000 >> batch_african_american_100k_log.out 2>&1 &

echo lgbt
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py lgbt 100000 >> batch_lgbt_updated_log.out 2>&1 &

echo parent
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py parent nonparents_100k >> batch_parent_updated_log.out 2>&1 &

echo vegan
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py vegan None >> batch_vegan_log.out 2>&1 &

echo female
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py female male >> batch_female_updated_log.out 2>&1 &

echo liberal
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py liberal conservative >> batch_liberal_updated_log.out 2>&1 &

echo christian
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py christian nonchristians >> batch_christian_updated_log.out 2>&1 &

echo jewish
nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/batch_predict_label.py jewish 100000 >> batch_jewish_updated_log.out 2>&1 &
