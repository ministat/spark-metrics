import dash
import dash_table
import glob
import os
import pandas as pd
import pathlib
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html

MATRIX_MAP_TO_TASK={
    "shuffleReadMetrics.remoteBytesRead":"taskMetrics.shuffleReadMetrics.remoteBytesRead",
    "shuffleWriteMetrics.writeBytes":"taskMetrics.shuffleWriteMetrics.bytesWritten",
    "inputMetrics.bytesRead":"taskMetrics.inputMetrics.bytesRead",
    "outputMetrics.bytesWritten":"taskMetrics.outputMetrics.bytesWritten"
}
LONG_NAME_TO_READABLE_NAME={
    "taskMetrics.shuffleReadMetrics.remoteBytesRead.min":"rShuf remo min",
    "taskMetrics.shuffleReadMetrics.remoteBytesRead.max":"rShuf remo max",
    "taskMetrics.shuffleReadMetrics.remoteBytesRead.min.host":"rShufH remo min",
    "taskMetrics.shuffleReadMetrics.remoteBytesRead.max.host":"rShufH remo max",
    "taskMetrics.shuffleWriteMetrics.bytesWritten.min":"wShuf min",
    "taskMetrics.shuffleWriteMetrics.bytesWritten.max":"wShuf max",
    "taskMetrics.shuffleWriteMetrics.bytesWritten.min.host":"wShuf min host",
    "taskMetrics.shuffleWriteMetrics.bytesWritten.max.host":"wShuf max host",
    "taskMetrics.inputMetrics.bytesRead.min":"rInput min",
    "taskMetrics.inputMetrics.bytesRead.max":"rInput max",
    "taskMetrics.inputMetrics.bytesRead.min.host":"rInput min host",
    "taskMetrics.inputMetrics.bytesRead.max.host":"rInput max host",
    "taskMetrics.outputMetrics.bytesWritten.min":"wOutput min",
    "taskMetrics.outputMetrics.bytesWritten.max":"wOutput max",
    "taskMetrics.outputMetrics.bytesWritten.min.host":"wOutput min host",
    "taskMetrics.outputMetrics.bytesWritten.max.host":"wOutput max host",
}

STAGE_SKEWS_FILE="stages_skew.csv"
SKEW_THRESHOLD = 100

# Colours
#color_1 = "#003399"
color_2 = "#00ffff"
#color_3 = "#002277"
color_b = "#F8F8FF"

MERGE_MIN_MAX = True

def extract_skew_info(skewDf, hostSkewsDf, stageDf, stageId, queue):
    selectedSkewStageDf = skewDf.loc[(skewDf["stageId"]==stageId)]
    selectedStageDf = stageDf.loc[(stageDf["stageId"]==stageId)]
    sqlDesc = selectedStageDf[["description"]].values[0][0] if len(selectedStageDf[["description"]]) > 0 else ""
    submitTime = selectedStageDf[["submissionTime"]].values[0][0]
    skewMetrics = []
    for c in selectedSkewStageDf.columns:
        if (c.endswith("bytesRead") or
            c.endswith("bytesWritten") or
            c.endswith("readBytes") or
            c.endswith("writeBytes") or
            c.endswith("remoteBytesRead") or
            c.endswith("remoteBytesReadToDisk")):
            v = selectedSkewStageDf[[c]]
            if v.values[0][0] > SKEW_THRESHOLD:
                skewMetrics.append(c)
    dic = {}
    dic["queue"] = queue
    dic["stageId"] = stageId
    dic["sql"] = sqlDesc
    dic["submissionTime"] = submitTime
    assert len(skewMetrics) > 0, "Fail to find skew metrics from task level metrics"
    for m in skewMetrics:
        if m in MATRIX_MAP_TO_TASK:
            tm = MATRIX_MAP_TO_TASK[m]
            # running stage does not contain those columns
            if not tm in hostSkewsDf:
                continue
            imax = hostSkewsDf[tm].idxmax()
            imin = hostSkewsDf[tm].idxmin()
            if MERGE_MIN_MAX is True:
                #if "max" in dic:
                #    dic["max"] += "," + tm
                #else:
                #    dic["max"] = tm
                #if "min" in dic:
                #    dic["min"] += "," + tm
                #else:
                #    dic["min"] = tm
                if "max.host" in dic:
                    dic["max.host"] += "," + hostSkewsDf.iloc[imax][['host']][0]
                else:
                    dic["max.host"] = hostSkewsDf.iloc[imax][['host']][0]
                if "min.host" in dic:
                    dic["min.host"] += "," + hostSkewsDf.iloc[imin][['host']][0]
                else:
                    dic["min.host"] = hostSkewsDf.iloc[imin][['host']][0]
                dic["{m}.max".format(m=tm)] = hostSkewsDf.iloc[imax][[tm]][0]
                dic["{m}.min".format(m=tm)] = hostSkewsDf.iloc[imin][[tm]][0]
            else:
                dic["{m}.max".format(m=tm)] = hostSkewsDf.iloc[imax][[tm]][0]
                dic["{m}.min".format(m=tm)] = hostSkewsDf.iloc[imin][[tm]][0]
                dic["{m}.max.host".format(m=tm)] = hostSkewsDf.iloc[imax][['host']][0]
                dic["{m}.min.host".format(m=tm)] = hostSkewsDf.iloc[imin][['host']][0]
    return dic

def gen_empty_data_skew(queue):
    dic = {}
    dic["queue"] = queue
    dic["stageId"] = "N/A"
    dic["sql"] = "N/A"
    dic["submissionTime"] = "N/A"
    if MERGE_MIN_MAX is True:
        dic["max"] = "N/A"
        dic["min"] = "N/A"
        dic["max.host"] = "N/A"
        dic["min.host"] = "N/A"
    else:
        dic["{m}.max".format(m=tm)] = hostSkewsDf.iloc[imax][[tm]][0]
        dic["{m}.min".format(m=tm)] = hostSkewsDf.iloc[imin][[tm]][0]
        dic["{m}.max.host".format(m=tm)] = hostSkewsDf.iloc[imax][['host']][0]
        dic["{m}.min.host".format(m=tm)] = hostSkewsDf.iloc[imin][['host']][0]
    df = pd.DataFrame()
    return df.append(pd.Series(dic), ignore_index=True)

def load_data_skew_stages(dataDir, selectedQueue):
    df = pd.DataFrame()
    for root, dirs, files in os.walk(dataDir):
        for file in files:
            if file.endswith("_data_skew.csv"):
                fnItems = file.split('_')
                pathItems = root.split(os.path.sep)
                assert len(pathItems) > 0, "Fail to find queue from {p}".format(p=root)
                queue = pathItems[len(pathItems) - 1]
                if selectedQueue != "all" and selectedQueue != queue:
                    continue
                if len(fnItems) < 3:
                    print("Invalid data skew file: {f}".format(f=file))
                    continue
                stage_skews = os.path.join(root, STAGE_SKEWS_FILE)
                if not os.path.exists(stage_skews):
                    print("Cannot find {s}".format(s=stage_skews))
                    continue
                stage_details = os.path.join(root, "*_stages.csv")
                stage_details_file = glob.glob(stage_details)
                if len(stage_details_file) != 1:
                    print("Cannot find {s}".format(s=stage_details_file))
                    continue
                print("{d}, {p}".format(d=root, p=fnItems[0]))
                hosts_skews = os.path.join(root, file)
                hostSkewDf = pd.read_csv(os.path.join(root, file))
                stageSkewDf = pd.read_csv(stage_skews)
                print(stage_details_file[0])
                detailsDf = pd.read_csv(stage_details_file[0], sep='|', lineterminator='\n')
                dic = extract_skew_info(stageSkewDf, hostSkewDf, detailsDf, int(fnItems[0]), queue)
                df = df.append(pd.Series(dic), ignore_index=True)
    for k,v in LONG_NAME_TO_READABLE_NAME.items():
        df = df.rename(columns={k:v})
    #print(df.columns)
    #df.to_csv("result.csv")
    return df

def get_data_skews_datatable(df):
    return dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records'),
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        #column_selectable="single",
        #row_selectable="multi",
        page_size = 10,
        style_header={
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold',
            'whileSpace': 'normal',
        },
        style_cell={
            'overflow': 'hidden',
            'textOverflow': 'ellipsis',
            'height': 'auto',
            'maxWidth': 100,
        },
        style_data_conditional=[
            {
                "if": {"row_index": "odd"},
                "backgroundColor": color_b,
            },
            {
                "if": {
                    "column_id": "Quarter"
                },
                "backgroundColor": color_2,
                "color": "black",
            },
        ],
        tooltip_data=[
            {
                column: {'value': str(value), 'type': 'markdown'}
                for column, value in row.items()
            } for row in df.to_dict('rows')
        ],
        tooltip_duration=None
    )
