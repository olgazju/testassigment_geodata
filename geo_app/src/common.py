import matplotlib.pyplot as plt

def save_pie_plot(proportions, labels, filename):

    fig1, ax1 = plt.subplots()
    ax1.pie(proportions, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    plt.savefig(filename)