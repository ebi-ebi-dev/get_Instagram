# get_Instagram
# 実行方法
```
python src\main.py -configfile_path "C:\Workspace\python\get_Instagram\configs\config_instagram.ini" -csv_path "C:\Workspace\python\get_Instagram\data" csv_prefix_name "pre_" csv_suffix_name "_suf" -sleep_sec 0.3
```
パラメータ
* configfile_path (必須)
ユーザーIDやアクセストークンなどの設定ファイルのパス
* csv_path (必須)
取得したデータを出力するフォルダパス
* csv_prefix_name (オプション)
出力するCSVファイルの先頭に付ける文字列
* csv_suffix_name (オプション)
出力するCSVファイルの末尾に付ける文字列
* sleep_sec (オプション)
APIをリクエストする間隔（秒）。APIの制限に引っかかる場合に0.3～1.0程度で指定する。