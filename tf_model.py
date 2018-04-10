from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import shutil
import sys

import tensorflow as tf  # pylint: disable=g-bad-import-order

from official.utils.arg_parsers import parsers
from official.utils.logs import hooks_helper

_CSV_COLUMNS = ['ticker', 'alpha', 'beta', 'trade_type', 'years_to_experiation', 'pct_capm_error',
    'pct_ma_50_volatility', 'stock_price', 'pct_strike_variance', 'option_price', 'pct_implied_volitility',
    'delta', 'gamma', 'theta', 'vega', 'rho', 'phi', 'label']
    
_CSV_COLUMN_DEFAULTS = [[''], [0], [0], [''], [0], [0],
                    [0], [0], [0], [0], [0],
                    [0], [0], [0], [0], [0], [0], ['']]

_NUM_EXAMPLES = {'train': 164316229, 'validation': 0}

LOSS_PREFIX = {'wide': 'linear/', 'deep': 'dnn/'}

def build_model_columns():
    ticker = tf.feature_column.categorical_column_with_hash_bucket('ticker', hash_bucket_size=10000)
    alpha = tf.feature_column.numeric_column('alpha')
    beta = tf.feature_column.numeric_column('beta')
    trade_type = tf.feature_column.categorical_column_with_vocabulary_list('trade_type', ['Long Call', 'Short Call', 'Long Put', 'Short Put'])
    years_to_experiation = tf.feature_column.numeric_column('years_to_experiation')
    pct_capm_error = tf.feature_column.numeric_column('pct_capm_error')
    pct_ma_50_volatility = tf.feature_column.numeric_column('pct_ma_50_volatility')
    stock_price = tf.feature_column.numeric_column('stock_price')
    pct_strike_variance = tf.feature_column.numeric_column('pct_strike_variance')
    option_price = tf.feature_column.numeric_column('option_price')
    pct_implied_volatility = tf.feature_column.numeric_column('pct_implied_volatility')
    delta = tf.feature_column.numeric_column('delta')
    gamma = tf.feature_column.numeric_column('gamma')
    theta = tf.feature_column.numeric_column('theta')
    vega = tf.feature_column.numeric_column('vega')
    rho = tf.feature_column.numeric_column('rho')
    phi = tf.feature_column.numeric_column('phi')
    label = tf.feature_column.categorical_column_with_hash_bucket('ticker', hash_bucket_size=65)
    
    wide_columns = [ticker, alpha, beta]
    
    deep_columns = [
        trade_type,
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
    
    return wide_columns, deep_columns

def build_estimator(model_dir, model_type):
    wide_columns, deep_columns = build_model_columns()
    hidden_units = [100, 75, 50, 25]
    
    run_config = tf.estimator.RunConfig()
    
    if model_type == 'wide':
        return tf.estimator.LinearClassifier(
            model_dir=model_dir,
            feature_columns=wide_columns,
            config=run_config)
    elif model_type == 'deep':
        return tf.estimator.DNNClassifier(
            model_dir=model_dir,
            feature_columns=deep_columns,
            hidden_units=hidden_units,
            config=run_config)
    else:
        return tf.estimator.DNNLinearCombinedClassifier(
            model_dir=model_dir,
            linear_feature_columns=wide_columns,
            dnn_feature_columns=deep_columns,
            dnn_hidden_units=hidden_units,
            config=run_config)

def input_fn(data_file, num_epochs, shuffle, batch_size):
    assert tf.gfile.Exists(data_file), ('%s no found. set the --data_dir argument to the correct path.' % data_file)
    
    def parse_csv(value):
        print('Parsing', data_file)
        columns = tf.decode_csv(value, record_defaults=_CSV_COLUMN_DEFAULTS)
        features = dict(zip(_CSV_COLUMNS, columns))
        labels = features.pop('label')
        return features, labels
    
    dataset = tf.data.TextLineDataset(data_file)
    
    if shuffle:
        dataset = dataset.shuffle(buffer_size=_NUM_EXAMPLES['train'])
    
    dataset = dataset.map(parse_csv, num_parrallel_calls=5)
    
    dataset = dataset.repeat(num_epochs)
    dataset = dataset.batch(batch_size)
    return dataset

def main(argv):
    parser = WideDeepArgParser()
    flags = parser.parse_args(args=argv[1:])
    
    # Clean up the model directory if present
    shutil.rmtree(flags.model_dir, ignore_errors=True)
    model = build_estimator(flags.model_dir, flags.model_type)
    
    train_file = os.path.join(flags.data_dir, 'training_data.csv')
    test_file = os.path.join(flags.data_dir, 'testing_data.csv')
    
    def train_input_fn():
        return input_fn(train_file, flags.epochs_between_evals, True, flags.batch_size)
        
    def eval_input_fn():
        return input_fn(test_file, 1, False, flags.batch_size)
        
    loss_prefix = LOSS_PREFIX.get(flags.model_type, '')
    train_hooks = hooks_helper.get_train_hooks(flags.hooks, batch_size=flags.batch_size, tensors_to_log={'average_loss': loss_prefix + 'head/truediv', 'loss': loss_prefix + 'head/weighted_loss/Sum'})
    
    # Train and evaluate the model every `flags.epochs_between_evals` epochs.
    for n in range(flags.train_epochs // flags.epochs_between_evals):
        model.train(input_fn=train_input_fn, hooks=train_hooks)
        results = model.evaluate(input_fn=eval_input_fn)
    
        print('Results at epoch', (n+1) * flags.epochs_between_evals)
        print('-' * 60)
        
        for key in sorted(results):
            print('%s: %s' % (key, results[key]))
            
class WideDeepArgParser(argparse.ArgumentParser):
    
    def __init__(self):
        super(WideDeepArgParser, self).__init__(parents=[parsers.BaseParser()])
        self.add_argument(
            '--model_type', '-mt', type=str, default='wide_deep'.
            choices=['wide', 'deep', 'wide_deep'],
            help='[default %(default)s] Valid model types: wide, deep, wide_deep.',
            metavar='<MT>')
        self.set_defaults(
            data_dir='/home/rayford/storage',
            model_dir='/home/rayford/code/forecasting_model',
            train_epochs=40,
            epochs_between_evals=2,
            batch_size=10000)
            
if __name__ == '__main__':
    tf.logging.set_verbosity(tf.logging.INFO)
    main(argv=sys.argv)
    
    
    