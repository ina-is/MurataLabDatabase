import json
import sqlite3
from typing import Dict, List, Tuple
import ast
from collections import defaultdict


conn = None

def connect():
    global conn
    conn = sqlite3.connect('./alr.db')


def close():
    conn.close()


def get(doc_id, fl):
    row_ls = conn.execute(
        'SELECT {} FROM docs WHERE id = ?'.format(','.join(fl)),
        (doc_id,)).fetchone()
    row_dict = {}
    for key, value in zip(fl, row_ls):
        row_dict[key] = value
    return row_dict


def get_all_ids(limit, offset=0):
    return [record[0] for record in
            conn.execute(
        'SELECT id FROM docs LIMIT ? OFFSET ?',
        (limit, offset))]


def get_annotation(doc_id, name):
    row = conn.execute(
        'SELECT {0} FROM docs WHERE id = ?'.format(name),
        (doc_id,)).fetchone()
    if row[0] is not None:
        return json.loads(row[0])
    else:
        return []
    
class AlrAnnotation():
    """
    データベースを扱うためのクラス
    """

    def __init__(self, doc_id: str):
        self.doc_id = doc_id

    def get_content(self) -> str:
        """
        トピック単位のテキストを取得

        Args:
            なし
        Returns:
            row['content'] (str): トピック単位のテキスト
        """

        row = get(self.doc_id, ['content'])
        return row['content']

    def get_metainfo(self) -> Tuple[int, str, str]:
        """
        id, 語り手（speaker），トピック（topic）を取得

        Args:
            なし
        Returns:
            id (int), speaker (str), topic (str)
        """

        row = get(self.doc_id, ['id', 'meta_info'])
        meta_info = ast.literal_eval(row['meta_info'])

        return row['id'], meta_info['speaker'], meta_info['topic']

    def get_annotation(self, anno_type: str) -> List[str]:
        """
        指定されたタイプのannotationの情報を返す

        Args:
            anno_type (str)
            'sentence', 'clause', 'chunk', 'token', 'response'
        Returns:
            datastore.get_annotation(self.doc_id, anno_type)
        Example:
            sentences = get_annotation('sentence')
        """

        return get_annotation(self.doc_id, anno_type)

    def output_annotation(self):
        """
        content, tokens. chunks, clauses, sentences の情報を出力．
        本来，class には不必要
        """

        print('content:')
        text = self.get_content()
        print(text)
        print('tokens:')
        tokens = self.get_annotation('token')
        for token in tokens:
            print('  ', token['POS'], '\t', text[token['begin']:token['end']])
            print('  ', token['starttime'], token['endtime'])
        print('chunks:')
        chunks = self.get_annotation('chunk')
        for chunk in chunks:
            _, link = chunk['link']
            print(
                '  ', chunk['starttime'], chunk['endtime'],
                text[chunk['begin']:chunk['end']])
            if link != -1:
                parent = chunks[link]
                print('\t-->', text[parent['begin']:parent['end']])
            else:
                print('\t-->', 'None')
        print('clauses:')
        clauses = self.get_annotation('clause')
        for clause in clauses:
            print(
                '  ', text[clause['begin']:clause['end']],
                '\t', clause['label'])
            print('  ', clause['starttime'], clause['endtime'])
        print('sentences:')
        sentences = self.get_annotation('sentence')
        for sent in sentences:
            print('  ', text[sent['begin']:sent['end']])
            print('  ', sent['starttime'], sent['endtime'])

    def output_response(self):
        """
        発話時間の制約を満たす傾聴応答の情報を出力．
        サンプルであり，本来，class には不必要
        """

        text = self.get_content()
        unit_type = 'clause'
        units = self.get_annotation(unit_type)

        responses = self.get_annotation('response')

        for unit in units:
            print(
                text[unit['begin']:unit['end']],
                '\t', unit['starttime'], '\t', unit['endtime'])

            for resp in responses:
                # 言語単位の発話時間内に発話が開始されている応答を表示
                if unit['starttime'] <= resp['starttime'] < unit['endtime']:
                    print(f"  {resp['listener']}, {resp['lemma']}, {resp['label']}, {resp['starttime']}, {resp['endtime']}")
                    # print(f"{text[resp['begin']:resp['end']]}, {resp['label']}, {resp['listener']}")

    def get_response(self) -> Dict[str, list]:
        """
        トピック単位の傾聴応答の情報を取得．
        目的とする処理のために情報を組み替える．

        Args:
            なし
        Returns:
            new_responses
        """

        new_responses = defaultdict(list)
        listener = 'o'  # database に 'o', 'a', 'b', ... の順番で格納しているので'o' から開始
        responses = self.get_annotation('response')
        for resp in responses:
            if listener != resp['listener']:
                pass
                # break  # 一人目である o さんの応答だけ取るなら break

            # all_responses_lemma[resp['label']].append(resp['lemma'])  # ラベルごとの傾聴応答文字列を dictionary で保管 -> TODO: 対応が必要

            # 対応付いた文節の位置情報
            duration = str(resp['begin']) + '-' + str(resp['end'])

            # 文節の位置情報をキーとしたリストの辞書を作成
            # output_for_classification, output_for_t5 ならこっち
            # Example: back-channel:うわー
            new_responses[duration].append(resp['label']+':'+resp['lemma'])
            # output_for_generationtimingならこっち
            # new_responses[duration].append(resp['listener'])

            # 情報の更新
            listener = resp['listener']

        return new_responses
    
    def get_start_end_times(self, listener_filter: str) -> List[Tuple[float, float]]:
        """
        指定されたlistenerの語り手のスタートタイムとレスポンスのエンドタイムを取得

        Args:
            listener_filter (str): フィルタリングするリスナー
        Returns:
            List[Tuple[float, float]]: 語り手のスタートタイムとレスポンスのエンドタイムのリスト
        """

        start_end_times = []
        units = self.get_annotation('clause')
        responses = self.get_annotation('response')

        for unit in units:
            start_time = float(unit['starttime'])
            for resp in responses:
                if unit['starttime'] <= resp['starttime'] < unit['endtime'] and resp['listener'] == listener_filter:
                    end_time = float(resp['endtime'])
                    start_end_times.append((start_time, end_time))

        return start_end_times

    def annotate_intervals(self, listener_filter: str) -> List[Tuple[float, float, int]]:
        """
        語り手のStartTimeとリスナーのEndTimeの時間を10[ms]ごとにリスナーのStartTimeでアノテーションする

        Args:
            listener_filter (str): フィルタリングするリスナー
        Returns:
            List[Tuple[float, float, int]]: 10ミリ秒ごとのラベル付き時間区間のリスト
        """

        intervals = []
        start_end_times = self.get_start_end_times(listener_filter)
        responses = self.get_annotation('response')

        for start_time, end_time in start_end_times:
            current_time = start_time
            while current_time < end_time:
                next_time = round(current_time + 0.01, 2)  # 10ミリ秒後
                label = 0
                for resp in responses:
                    if float(resp['starttime']) == current_time and resp['listener'] == listener_filter:
                        label = 1
                        break
                intervals.append((current_time, next_time, label))
                current_time = next_time

        return intervals

connect()

# ここまでが事前準備


alr_anno = AlrAnnotation(1)  # id を引数にしてインスタンスを作成



# 特定のリスナーの語り手のスタートタイムとレスポンスのエンドタイムの抽出
listener_filter = 'o'  # フィルタリングするリスナーを指定
intervals = alr_anno.annotate_intervals(listener_filter)
print(f"10ミリ秒ごとのアノテーションされた時間区間:")
for start, end, label in intervals:
    print(f"Start Time: {start}, End Time: {end}, Label: {label}")


close()