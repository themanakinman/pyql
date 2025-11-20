from flask import Flask, render_template, request, jsonify
import sys
import os
import math

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__,
            static_folder=os.path.join(basedir, 'static'),
            template_folder=os.path.join(basedir, 'templates'))
loadedDataFrames = {}

from pyql import DataFrame, compare


def limit_columns(data_dict, max_columns=10):
    """
    limit the number of columns returned
    
    args:
        data_dict: dictionary of column data
        max_columns: maximum number of columns to return
    
    returns:
        tuple of (limited_data, total_column_count)
    """
    all_columns = list(data_dict.keys())
    total_columns = len(all_columns)
    
    if total_columns <= max_columns:
        return data_dict, total_columns
    
    # take first max_columns
    limited_data = {col: data_dict[col] for col in all_columns[:max_columns]}
    
    return limited_data, total_columns

def clean_data_for_json(data_dict):
    """replace inf, -inf, and nan with none for valid json"""
    cleaned = {}
    for column, values in data_dict.items():
        cleaned[column] = []
        for value in values:
            # check if value is a float and handle special cases
            if isinstance(value, float):
                if math.isinf(value) or math.isnan(value):
                    cleaned[column].append(None)
                else:
                    cleaned[column].append(value)
            else:
                cleaned[column].append(value)
    return cleaned

@app.route('/')
def index():
    """landing page"""
    return render_template('index.html')

@app.route('/api/load', methods=['POST'])
def load_data():
    """load csv file into dataframe"""
    try:
        data = request.get_json()
        filepath = data.get('filepath')
        name = data.get('name', 'df')
        
        if not filepath:
            return jsonify({'error': 'No filepath provided'}), 400
        
        if not os.path.isabs(filepath):
            project_root = os.path.abspath(os.path.join(basedir, '..'))
            filepath = os.path.join(project_root, filepath)
        
        df = DataFrame.fromCSV(filepath)
        loadedDataFrames[name] = df
        
        # get preview and limit columns
        preview_full = df.head(10).toDict()
        preview_cleaned = clean_data_for_json(preview_full)
        preview_limited, total_cols = limit_columns(preview_cleaned, max_columns=10)
        
        return jsonify({
            'success': True,
            'name': name,
            'rows': len(df),
            'columns': df.columns[:10],  # return only first 10 column names
            'total_columns': len(df.columns),  # total column count
            'preview': preview_limited
        })
    
    except FileNotFoundError:
        return jsonify({'error': f'File not found: {filepath}'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/filter', methods=['POST'])
def filter_data():
    """filter dataframe based on conditions"""
    try:
        data = request.get_json()
        df_name = data.get('dataframe', 'df')
        
        # support both single and multiple filters
        filters = data.get('filters')
        logic_op = data.get('logic', 'and')
        
        if df_name not in loadedDataFrames:
            return jsonify({'error': f'DataFrame "{df_name}" not loaded'}), 404
        
        df = loadedDataFrames[df_name]
        
        # handle multiple filters
        if isinstance(filters, list) and len(filters) > 0:
            masks = []
            
            for f in filters:
                column = f.get('column')
                operator = f.get('operator')
                value = f.get('value')
                
                # convert value to appropriate type
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                
                mask = compare(df, column, operator, value)
                masks.append(mask)
            
            # combine masks
            combined_mask = masks[0]
            for mask in masks[1:]:
                if logic_op == 'and':
                    combined_mask = combined_mask & mask
                else:
                    combined_mask = combined_mask | mask
            
            result_df = df[combined_mask]
        else:
            # single filter fallback
            column = data.get('column')
            operator = data.get('operator')
            value = data.get('value')
            
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass
            
            mask = compare(df, column, operator, value)
            result_df = df[mask]
        
        # clean and limit columns
        result_dict = result_df.toDict()
        result_cleaned = clean_data_for_json(result_dict)
        result_limited, total_cols = limit_columns(result_cleaned, max_columns=10)
        
        return jsonify({
            'success': True,
            'rows': len(result_df),
            'total_columns': total_cols,
            'displayed_columns': len(result_limited),
            'data': result_limited
        })
    
    except KeyError as e:
        return jsonify({'error': f'Column not found: {str(e)}'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/aggregate-simple', methods=['POST'])
def aggregate_simple():
    """simple aggregation without grouping"""
    try:
        data = request.get_json()
        df_name = data.get('dataframe', 'df')
        column = data.get('column')
        func = data.get('function')
        
        if df_name not in loadedDataFrames:
            return jsonify({'error': f'DataFrame "{df_name}" not loaded'}), 404
        
        df = loadedDataFrames[df_name]
        
        if column not in df.columns:
            return jsonify({'error': f'Column "{column}" not found'}), 400
        
        # perform aggregation
        if func == 'sum':
            result = df.sum(column)
        elif func == 'mean':
            result = df.mean(column)
        elif func == 'max':
            result = df.max(column)
        elif func == 'min':
            result = df.min(column)
        elif func == 'count':
            result = df.count(column)
        else:
            return jsonify({'error': f'Unknown function: {func}'}), 400
        
        return jsonify({
            'success': True,
            'result': result,
            'row_count': len(df)
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/aggregate', methods=['POST'])
def aggregate_data():
    """grouped aggregation"""
    try:
        data = request.get_json()
        df_name = data.get('dataframe', 'df')
        group_by = data.get('groupby')
        agg_column = data.get('column')
        agg_func = data.get('function')
        
        if df_name not in loadedDataFrames:
            return jsonify({'error': f'DataFrame "{df_name}" not loaded'}), 404
        
        df = loadedDataFrames[df_name]
        grouped = df.groupBy(group_by)
        result_df = grouped.agg({agg_column: agg_func})
        
        return jsonify({
            'success': True,
            'rows': len(result_df),
            'data': result_df.toDict()
        })
    
    except KeyError as e:
        return jsonify({'error': f'Column not found: {str(e)}'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/join', methods=['POST'])
def join_data():
    """join two dataframes"""
    try:
        data = request.get_json()
        left_df_name = data.get('left')
        right_df_name = data.get('right')
        left_on = data.get('left_on')
        right_on = data.get('right_on')
        how = data.get('how', 'inner')
        
        if left_df_name not in loadedDataFrames:
            return jsonify({'error': f'DataFrame "{left_df_name}" not loaded'}), 404
        if right_df_name not in loadedDataFrames:
            return jsonify({'error': f'DataFrame "{right_df_name}" not loaded'}), 404
        
        left_df = loadedDataFrames[left_df_name]
        right_df = loadedDataFrames[right_df_name]
        result_df = left_df.merge(right_df, leftOn=left_on, rightOn=right_on, how=how)
        
        return jsonify({
            'success': True,
            'rows': len(result_df),
            'columns': result_df.columns,
            'data': result_df.toDict()
        })
    
    except KeyError as e:
        return jsonify({'error': f'Column not found: {str(e)}'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/select', methods=['POST'])
def select_columns():
    """select specific columns from dataframe"""
    try:
        data = request.get_json()
        df_name = data.get('dataframe', 'df')
        columns = data.get('columns')
        
        if df_name not in loadedDataFrames:
            return jsonify({'error': f'DataFrame "{df_name}" not loaded'}), 404
        
        df = loadedDataFrames[df_name]
        result_df = df[columns]
        
        return jsonify({
            'success': True,
            'rows': len(result_df),
            'data': result_df.toDict()
        })
    
    except KeyError as e:
        return jsonify({'error': f'Column not found: {str(e)}'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dataframes', methods=['GET'])
def list_dataframes():
    """list all loaded dataframes"""
    return jsonify({
        'dataframes': {
            name: {
                'rows': len(df),
                'columns': df.columns
            }
            for name, df in loadedDataFrames.items()
        }
    })

@app.route('/api/clear', methods=['POST'])
def clear_dataframes():
    """clear all loaded dataframes"""
    global loadedDataFrames
    loadedDataFrames = {}
    return jsonify({'success': True, 'message': 'All DataFrames cleared'})

@app.route('/api/info/<df_name>', methods=['GET'])
def dataframe_info(df_name):
    """get information about a specific dataframe"""
    if df_name not in loadedDataFrames:
        return jsonify({'error': f'DataFrame "{df_name}" not loaded'}), 404
    
    df = loadedDataFrames[df_name]
    return jsonify({
        'name': df_name,
        'rows': len(df),
        'columns': df.columns,
        'shape': df.shape(),
        'preview': df.head(5).toDict()
    })

if __name__ == '__main__':
    print(f"Server running at: http://localhost:3000")
    app.run(debug=True, port=3000, host='0.0.0.0')
