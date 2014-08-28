label=$1
script_dir='~/analytics_and_reporting/text_models'

sudo mkdir /data/user_documents/individual_posts_${label}

nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/get_individual_aboutme_documents.py $label &

nohup sudo ~/virtualenvs/py27/bin/python ${script_dir}/get_individual_post_documents.py $label &
