import tensorflow as tf
import shutil

#whoops, pct_implied_volitility spelled wrong. Fixed in query but when new dataset is made will have to update spellings below.
_CSV_COLUMNS = ['ticker', 'alpha', 'beta', 'trade_type', 'years_to_experiation', 'pct_capm_error',
    'pct_ma_50_volatility', 'stock_price', 'pct_strike_variance', 'option_price', 'pct_implied_volitility',
    'delta', 'gamma', 'theta', 'vega', 'rho', 'phi', 'label']

LABEL_VOCAB = ['-51', '-52', '-53', '-54', '-55', '-56', '-57',
               '-41', '-42', '-43', '-44', '-45', '-46', '-47',
               '-31', '-32', '-33', '-34', '-35', '-36', '-37',
               '-21', '-22', '-23', '-24', '-25', '-26', '-27',
               '-11', '-12', '-13', '-14', '-15', '-16', '-17',
               '11', '12', '13', '14', '15', '16', '17',
               '21', '22', '23', '24', '25', '26', '27',
               '31', '32', '33', '34', '35', '36', '37',
               '41', '42', '43', '44', '45', '46', '47',
               '51', '52', '53', '54', '55', '56', '57']

#last default for label has to be an int data type since after we convert labels to one-hots below the one-hot will hold ints and tensorflow cares.
_CSV_COLUMN_DEFAULTS = [[''], [0.0], [0.0], [''], [0.0], [0.0],
                        [0.0], [0.0], [0.0], [0.0], [0.0],
                        [0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0]]

def build_feature_columns():
    ticker = tf.feature_column.categorical_column_with_hash_bucket('ticker', hash_bucket_size=10000)
    alpha = tf.feature_column.numeric_column('alpha', dtype=tf.float32)
    beta = tf.feature_column.numeric_column('beta', dtype=tf.float32)
    trade_type = tf.feature_column.categorical_column_with_vocabulary_list('trade_type', ['Long Call', 'Short Call', 'Long Put', 'Short Put'])
    years_to_experiation = tf.feature_column.numeric_column('years_to_experiation', dtype=tf.float32)
    pct_capm_error = tf.feature_column.numeric_column('pct_capm_error', dtype=tf.float32)
    pct_ma_50_volatility = tf.feature_column.numeric_column('pct_ma_50_volatility', dtype=tf.float32)
    stock_price = tf.feature_column.numeric_column('stock_price', dtype=tf.float32)
    pct_strike_variance = tf.feature_column.numeric_column('pct_strike_variance', dtype=tf.float32)
    option_price = tf.feature_column.numeric_column('option_price', dtype=tf.float32)
    pct_implied_volatility = tf.feature_column.numeric_column('pct_implied_volitility', dtype=tf.float32)
    delta = tf.feature_column.numeric_column('delta', dtype=tf.float32)
    gamma = tf.feature_column.numeric_column('gamma', dtype=tf.float32)
    theta = tf.feature_column.numeric_column('theta', dtype=tf.float32)
    vega = tf.feature_column.numeric_column('vega', dtype=tf.float32)
    rho = tf.feature_column.numeric_column('rho', dtype=tf.float32)
    phi = tf.feature_column.numeric_column('phi', dtype=tf.float32)

    feature_columns = [
                tf.feature_column.indicator_column(ticker),
                alpha,
                beta,
                tf.feature_column.indicator_column(trade_type),
                years_to_experiation,
                pct_capm_error,
                pct_ma_50_volatility,
                stock_price,
                pct_strike_variance,
                option_price,
                pct_implied_volatility,
                delta,
                gamma,
                theta,
                vega,
                rho,
                phi]

    return feature_columns

def input_fn(data_file, num_epochs, shuffle, batch_size):
    assert tf.gfile.Exists(data_file), ('%s no found. set the --data_dir argument to the correct path.' % data_file)

    def parse_csv(value):
        print('Parsing', data_file)
        columns = tf.decode_csv(value, record_defaults=_CSV_COLUMN_DEFAULTS)
        features = dict(zip(_CSV_COLUMNS, columns))
        try:
            labels = tf.one_hot(LABEL_VOCAB.index(features.pop('label').eval()), len(LABEL_VOCAB))
        except:
            print('-' * 60)
            print('Error Encoding Label')
            labels = tf.one_hot(1, len(LABEL_VOCAB))

        return features, labels

    dataset = tf.data.TextLineDataset(data_file).skip(1)

    if shuffle:
        dataset = dataset.shuffle(buffer_size=100000000)

    dataset = dataset.map(parse_csv, num_parallel_calls=6)
    dataset = dataset.repeat(num_epochs)
    dataset = dataset.batch(batch_size)
    return dataset.make_one_shot_iterator().get_next()

def model_fn(features, labels, mode, params):
    #input layer
    input_layer = tf.feature_column.input_layer(features, params['feature_columns'])

    #hidden layers
    hidden1 = tf.layers.dense(inputs=input_layer, units=128, activation=tf.nn.relu)
    hidden2 = tf.layers.dense(inputs=hidden1, units=256, activation=tf.nn.relu)
    hidden3 = tf.layers.dense(inputs=hidden2, units=128, activation=tf.nn.relu)

    #logits
    logits = tf.layers.dense(inputs=hidden3, units=len(LABEL_VOCAB), activation=None)

    #compute predictions
    predicted_classes = tf.argmax(logits, 1)
    if mode == tf.estimator.ModeKeys.PREDICT:
        predictions = {
            'class_ids': predicted_classes[:, tf.newaxis],
            'probabilities': tf.nn.softmax(logits),
            'logits': logits,
        }
        return tf.estimator.EstimatorSpec(mode, predictions=predictions)

    #compute loss
    loss = tf.losses.softmax_cross_entropy(onehot_labels=labels, logits=logits)

    #compute evaluation metrics
    accuracy = tf.metrics.accuracy(labels=labels, predictions=predicted_classes, name='acc_op')

    metrics = {'accuracy': accuracy}
    tf.summary.scalar('accuracy', accuracy)

    optimizer = tf.train.AdagradOptimizer(learning_rate=0.1)
    train_op = optimizer.minimize(loss, global_step=tf.train.get_global_step())

    return tf.estimator.EstimatorSpec(mode, loss=loss, train_op=train_op_)

def main(argv):
    # Clean up the model directory if present
    shutil.rmtree('/home/rayford/code/market/forecasting_model/run1', ignore_errors=True)

    feature_columns = build_feature_columns()

    def train_input_fn():
        return input_fn('/home/rayford/code/tensorflow_data/training_data.csv', 1, True, 1000)

    def eval_input_fn():
        return input_fn('home/rayford/code/tensorflow_data/testing_data.csv', 1, False, 1000)

    classifier = tf.estimator.Estimator(
        model_fn=model_fn,
        model_dir='/home/rayford/code/market/forecasting_model/run1',
        params={
            'feature_columns': feature_columns,
        })

    #train the model
    classifier.train(input_fn=train_input_fn, steps=160000)

    #evaluate the model
    eval_result = classifier.evaluate(input_fn=eval_input_fn)

    print('\nTest Set Accuracy: {accuracy:0.3f}\n'.format(**eval_result))


if __name__ == '__main__':
    tf.logging.set_verbosity(tf.logging.INFO)
    tf.app.run()
