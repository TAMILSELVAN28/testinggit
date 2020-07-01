# Default Packages
import time
import re
import sys
import json
import pickle
from datetime import timedelta
# import datetime

# Installed Packages
import urllib.parse
import requests
from flask import Flask, request, Response, session
import sentry_sdk

# Internal Files
import constants
from translator import Translator
from logger import log, set_transaction_details, close_pubsub_client

sys.path.append('../../helpers')
# from special_char_handling import SpecialCharHandler
# special_char_handling_obj = SpecialCharHandler(constants.SPECIAL_CHAR_PICKLE_PATH)

with open(constants.KB_TRIE_PATH, 'rb') as in_file:
    KB_TRIE = pickle.load(in_file)

sentry_sdk.init(constants.SENTRY_URL)

HOST = 'X-Real-Host'
AUTHORIZATION = 'Authorization'
QUESTION = 'question'


app = Flask('Search')
app.secret_key = 'any string'
app.permanent_session_lifetime = timedelta(minutes=0.5)

def respond(status_code, response_data=None):
    print("here")
    return Response(response_data, status=status_code, content_type='application/json')

def authenticator(headers):
    # Authenticate
    user_data = requests.post(
        f"https://{headers[HOST]}/authenticate",
        headers={
            AUTHORIZATION: headers[AUTHORIZATION], "Content-Type": 'application/json',
        }
    )
    return user_data

def authorizer(headers, payload):
    db_creds = requests.post(
        f"https://{headers[HOST]}/cgauthorize",
        data=json.dumps(payload)
    )
    if db_creds.status_code != 200:
        print("Authorization failed")
        log('authorization', {'reason': "Authorization failed"}, status=2)
        return respond(db_creds.status_code)
    return db_creds

@app.route("/health", methods=['GET'])
def health():
    return respond(
        200,
        response_data=bytes(
            json.dumps(
                "hey... its me the search...iam up and healthy :d",
                default=str), 'utf-8')
    )


@app.route("/solve_es", methods=['GET'])
@app.route("/solve", methods=['GET'])
def solve():
    try:
        start_time = time.time()
        query = request.args

        # TEMPORARY
        search_type = query['location']
        headers = dict(request.headers)

        if AUTHORIZATION not in headers or HOST not in headers:
            set_transaction_details()
            log('authentication', {'reason': "Insufficient headers"}, status=2)
            return respond(
                400,
                response_data=bytes(
                    json.dumps({'error': "Unable to resolve user"}, default=str), 'utf-8'
                )
            )

        user_data = authenticator(headers)
        if user_data.status_code != 200:
            set_transaction_details()
            log('auth-failed', {'reason': "Authentication Failed"}, status=2)
            return respond(user_data.status_code)

        # user_id, tenant_id = 'YwqYVDrSdpQ5aKebE5xxh60S7P42', 'amgentest-dwkd7'
        user_data = user_data.json()
        user_id, tenant_id, email_id = \
            user_data["user_id"], user_data["tenant_id"], user_data['email_id']

        set_transaction_details(tenant_id, user_id)

        # Authorize
        payload = {
            "user_id": user_id,
            "api_policies": json.dumps(constants.POLICIES),
            "required_creds": json.dumps(constants.CREDS_REQUIRED)
        }
        
        db_creds = authorizer(headers, payload)

        db_creds = db_creds.json()
        question = query[QUESTION]

        log(
            'trans-details', {
                "link": f"https://{headers[HOST]}/search/#/solve/search/{urllib.parse.quote(question, safe='')}",
                "email": email_id
            }
        )

        doc_type = None
        if ':' in question and question.split(':', 1)[0].lower() in constants.SUPPORTED_DOC_TYPES:
            parsed_question = question.split(':', 1)
            doc_type = parsed_question[0].lower()
            question = parsed_question[1]

        print("Question:", question)
        log('-', question)

        # Question text clean up
        # question = special_char_handling_obj.handle_all_special_chars(question)
        # TEMPORARY
        question = question.replace(",", " ")
        question = question.replace("'s ", " ")
        question = question.replace(".", " ")
        question = question.strip("[? ]")
        question = re.sub(r" +", " ", question).strip()
        log('pre-process', question)

        translator = None
        try:
            if search_type == 'app':
                translator = Translator(
                    KB_TRIE, tenant_id, user_id, constants.USER_LEVEL_POLICY, question, doc_type
                )
            else:
                translator = Translator(
                    KB_TRIE, tenant_id, user_id, db_creds['policy'], question, doc_type
                )
            queries_formed = translator.form_queries(session)
            session.modified = True
            session.permanent = True

            if not queries_formed:
                response = respond(
                    400,
                    response_data=bytes("No meaningful word found in question", 'utf-8')
                )
            else:
                save_time = time.time()
                translator.initialize_db(db_creds)
                translator.save_queries()
                timesd = time.time() - save_time
                translator.execute_queries()
                data = translator.format_response(search_type, KB_TRIE)
                # data['response']['savequerytime'] = timesd
                data['response']['result'] = session
                response = respond(
                    200,
                    response_data=bytes(json.dumps(data['response'], default=str), 'utf-8')
                )
            log('end-flag', status=0, time=time.time() - start_time)
            return response
        except Exception:
            log('end-flag', status=1, time=time.time() - start_time)
            raise

    except Exception as e:
        raise
        sentry_sdk.capture_exception(e)
        # raise(e)
        return respond(502)

    finally:
        close_pubsub_client()
  

@app.route("/pagination", methods=['GET'])
def pagination():
    try:
        start_time = time.time()
        query = request.args
        trans_id = query['trans_id']
        offset = query['offset']
        offset = int(offset) * 10
        print(offset)

        # TEMPORARY
        search_type = query['location']
        headers = dict(request.headers)

        if AUTHORIZATION not in headers or HOST not in headers:
            set_transaction_details(trans_id=trans_id)
            log('authentication', {'reason': "Insufficient headers"}, status=2)
            return respond(
                400,
                response_data=bytes(
                    json.dumps({'error': "Unable to resolve user"}, default=str), 'utf-8'
                )
            )
        
        user_data = authenticator(headers)
        if user_data.status_code != 200:
            set_transaction_details(trans_id=trans_id)
            log('auth-failed', {'reason': "Authentication Failed"}, status=2)
            return respond(user_data.status_code)

        # user_id, tenant_id = 'YwqYVDrSdpQ5aKebE5xxh60S7P42', 'amgentest-dwkd7'
        user_data = user_data.json()
        user_id, tenant_id, email_id = \
            user_data["user_id"], user_data["tenant_id"], user_data['email_id']

        set_transaction_details(tenant_id, user_id, trans_id=trans_id)

        # Authorize
        payload = {
            "user_id": user_id,
            "api_policies": json.dumps(constants.POLICIES),
            "required_creds": json.dumps(constants.CREDS_REQUIRED)
        }
        db_creds = authorizer(headers, payload)
        db_creds = db_creds.json()

        log(
            'trans-details', {
                "link": f"https://{headers[HOST]}/search/#/pagination//{urllib.parse.quote(trans_id, safe='')}",
                "email": email_id
            }
        )
        fetch_redis_query = time.time()
        doc_type = None
        translator = None
        try:
            if search_type == 'app':
                translator = Translator(
                    KB_TRIE, tenant_id, user_id, constants.USER_LEVEL_POLICY, doc_type
                )
            else:
                translator = Translator(
                    KB_TRIE, tenant_id, user_id, db_creds['policy'], doc_type
                )
            translator.initialize_db(db_creds)
            queries_formed = translator.fetch_queries(trans_id, offset)
            stored_time = time.time() - fetch_redis_query
            print(stored_time)
            if not queries_formed:
                response = respond(
                    400,
                    response_data=bytes("Query not found in redis", 'utf-8')
                )
            else:
                translator.execute_queries()
                data = translator.format_response(search_type, KB_TRIE)
                # data['response']['redisstored'] = stored_time
                data['response']['result'] = session
                # print(session['response'])
                response = respond(
                    200,
                    response_data=bytes(json.dumps(data['response'], default=str), 'utf-8')
                )
            log('end-flag', status=0, time=time.time() - start_time)
            return response
        except Exception:
            log('end-flag', status=1, time=time.time() - start_time)
            raise

        finally:
            if translator:
                translator.close()

    except Exception as e:
        raise
        sentry_sdk.capture_exception(e)
        # raise(e)
        return respond(502)

    # finally:
    #     close_pubsub_client()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=constants.PORT_NUMBER, debug=True)

