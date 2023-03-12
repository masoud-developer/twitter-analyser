from flask import Flask
from flask_restful import Api
import app_controller
from background_worker import BackgroundWorker
import nltk


if __name__ == '__main__':
    app = Flask(__name__)
    api = Api(app)
    routs = ['/accounts', '/tweets/<account>', '/audience/<account>', '/sentiment/<account>']
    api.add_resource(app_controller.AppController, *routs)

    nltk.download('vader_lexicon')
    nltk.download('stopwords')
    nltk.download('punkt')
    worker = BackgroundWorker()
    worker.start()

    app.run(debug=False, host='0.0.0.0', port=8099)
