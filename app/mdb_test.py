import db_connector
import plotly.graph_objects as go
from pathlib import Path
import datetime as dt

def plot_measures(df, ylabel="", title="", label_names=None, legend_title=""):
    """General plotting function
    Takes a dataframe and plots each column as a line on the same graph
    """
    
    fig = go.Figure()

    for id, column in enumerate(df.columns):

        if label_names is not None:
            label = label_names[id]
        else:
            label = column

        fig.add_trace(go.Scatter(
            x=df.index,  # X-axis (Timestamp)
            y=df[column],  # Y-axis (Temperature values)
            mode='lines+markers',  # Lines and markers
            name=label,  # Label for each line (Depth sensor)
            marker=dict(size=6)  # Marker customization
        ))

    fig.update_layout(
        title=title,
        xaxis_title='Time',
        yaxis_title=ylabel,
        legend_title=legend_title,
        template='plotly_white'
    )

    return fig




dev_eui = 'feedbeefcafe0004'
mast = 'Mast 12'

start, stop = None, None

print('Requesting data from database...')
df_gt, sensor_depth_dict, gt_dt_obj = db_connector.get_ground_temp(dev_eui, start, stop)
print('Ground temperature data received.')
df_weather = db_connector.get_sensor_data(dev_eui, "Weather Sensor", start, stop)
print('Weather data received.')
df_incl = db_connector.get_sensor_data(dev_eui, "Inclination Sensor", start, stop)
print('Inclination data received.')

print('Plotting data...')
fig_gt = plot_measures(df_gt, ylabel="Temperature (°C)", title=f"Ground Temperature - {mast}", label_names=[f"{sensor_depth_dict[column]:+0.2f} m" for column in df_gt.columns], legend_title="Sensor Depths")   
fig_airtemp = plot_measures(df_weather[['AirTemp']], ylabel="Temperature (°C)", title=f"Air Temperature - {mast}", label_names=["Air Temperature"], legend_title="Measurement")
fig_rh = plot_measures(df_weather[['RelHum']], ylabel="Relative Humidity (%)", title=f"Relative Humidity - {mast}", label_names=["Relative Humidity"], legend_title="Measurement")
fig_bp = plot_measures(df_weather[['BarometricPressure']], ylabel="Pressure (kPa)", title=f"Barometric Pressure - {mast}", label_names=["Barometric Pressure"], legend_title="Measurement")
fig_incl = plot_measures(df_incl, ylabel="Inclination (deg)", title=f"Inclination - {mast}", legend_title="Measurement")

print('Saving images...')
Path("images").mkdir(parents=True, exist_ok=True)
timestr = dt.datetime.now().strftime('%Y%m%d%H%M%S')

fname = Path("images") / f"fig_gt_{timestr}.png"
fig_gt.write_image(str(fname))

fname = Path("images") / f"fig_airtemp_{timestr}.png"
fig_airtemp.write_image(str(fname))

fname = Path("images") / f"fig_rh_{timestr}.png"
fig_rh.write_image(str(fname))

fname = Path("images") / f"fig_bp_{timestr}.png"
fig_bp.write_image(str(fname))

fname = Path("images") / f"fig_incl_{timestr}.png"
fig_incl.write_image(str(fname))

print('Done.')
