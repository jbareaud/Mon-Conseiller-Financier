import numpy as np
import plotly.express as px

# Génération de données aléatoires suivant une loi normale
np.random.seed(42)
data_x = [0, 4]
data_y = [4.91, 3.92]

fig = px.histogram(
    x=data_x,
    y=data_y,
    nbins=6,
    title="Histogramme",
    labels={"x": "Epoch", "y": "Perplexity"},
    opacity=0.8,
)

fig.update_layout(
    bargap=0.1,
    xaxis_title="Epoch",
    yaxis_title="Perplexity",
)

fig.show()