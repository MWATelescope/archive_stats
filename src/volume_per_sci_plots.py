#!/usr/bin/env python

import argparse
import numpy as np
import matplotlib.pyplot as plt
import os
import csv

parser = argparse.ArgumentParser(description='Plot archive usage broken down by'
                                  ' science theme.')
parser.add_argument('infile', type=str,
                    help='CSV file with storage data per project ID.')
parser.add_argument('--outdir', type=str, default=None,
                    help='Output directory for plots. Default is current directory.')
parser.add_argument('--print_stats', help='Print some stats to the screen.',
                    action='store_true')
parser.add_argument('--ext', default='png', type=str,
                    help='Plot file extension. Default is png.')

args = parser.parse_args()

if args.outdir is None:
    args.outdir = os.path.curdir

data = []
with open(args.infile, newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    next(csvreader)
    for row in csvreader:
        data.append(row)
data = np.array(data)

order = np.argsort(data[:, 2].astype(float))[-1::-1]
ids = data[order, 0]
descs = data[order, 1]
vols = data[order, 2].astype(float)

# Dictionary to hold mapping from project to science theme (Note incomplete)
cat_lookup = ['EoR', 'SHI', 'GEG', 'Transients', 'Pulsars and FT', 'Calibration', 'Misc']
cat_lookup_align = ['EoR\t', 'SHI\t', 'GEG\t', 'Transients', 'Pulsars and FT',
                    'Calibration', 'Misc\t']
cats = {"Sun Drifts": 1,
        "GLEAM": 2,
        "FRBs with the MWA": 4,
        "Shadowing Parkes FRB observations": 4,
        "Solar Observations": 1,
        "EoR": 0,
        "Calibration": 5,
        "MWA pulsar survey": 4,
        "Low-freq investigations of Parkes pta MSP": 4,
        "Unspecified Director's time": 6,
        "Global EoR with the moon": 0,
        "Voltage capture testing": 4,
        "IPS survey": 3,
        "Synchrotron Cosmic Web": 2,
        "MIDAS": 2,
        "HIghZ: A search for HI absorption in high-redshift radio galaxies": 2,
        "IPS": 3,
        "MWA targeted campaign of nearby, flaring M dwarf stars": 2,
        "Shadowing Parkes FRB observations": 4,
        "AAVS0.5 tests": 6,
        "MAGE­‐X: A Deep Survey of the Magellanic System": 2,
        "EoR SKA Fields": 0,
        "The MWA long-term radio sky monitor": 3,
        "Cosmic web observation": 2,
        "Detecting Molecular Lines with the MWA": 2,
        "MWA KAT-7 clusters": 2,
        "Intermediate dispersion pulsar scattering": 4,
        "MWA diffuse cluster emission": 2,
        "Follow up observations of UV Ceti": 3,
        "Default": 6,
        "FAST pulsar candidate": 4,
        "Orbital dynamics of PSR J1145-6545": 4,
        "Dispersion variation in J2241-5236": 4,
        "Proxima Centauri": 2,
        "Space Situational Awareness (VCS)": 4,
        "Radio pules from the Geminga Pulsar": 4,
        "Instrument Verification Program": 6,
        "Monitoring the Galaxy": 3,
        "RRLs in GC and NGC6334": 2,
        "IPS observations for SKA calibration analysis": 3,
        "Nearby Low-Luminosity QSOs": 2,
        "Searching for pulsars in the image domain: pilot study": 3}

nproj = len(vols)
cum_frac = np.zeros(nproj)
for i in range(nproj):
    cum_frac[i] = np.sum(vols[:i + 1]) / vols.sum()

cat_totals = np.zeros(len(cat_lookup))
for i, key in enumerate(cats.keys()):
    cat_totals[cats[key]] += vols[i]


if args.print_stats:
    print(f'\nTotal number of projects: {nproj}')
    print(f'Total volume of archive: {np.sum(vols)} TB')
    print('\n')
    print('SWG\t\tVolume (TB)')
    for i, cat in enumerate(cat_lookup_align):
        print(f'{cat}\t{cat_totals[i]}')
    print('Volume fraction captured by my incomplete categorization: '
          f'{np.sum(cat_totals) / np.sum(vols)}')
    print('\n')
    print('Largest projects:')
    print('ID\tVolume (TB)\tSWG\tDescription')
    for id, desc, vol in zip(ids, descs, vols):
        try:
            cat = cat_lookup_align[cats[desc.strip(' ')]]
        except KeyError:
            cat = '\t'
        print(f'{id}\t{vol}\t{cat}\t{desc}')

# Cumulative fraction plot
fig, ax1 = plt.subplots()
ax1.set_ylabel('Cumulative Archive Fraction')
ax1.plot(cum_frac)
# Set right axis
ax2 = ax1.twinx()
ax2.set_ylabel('Cumulative Archive Volume (TB)')
ax2.set_ylim([0, ax1.get_ylim()[1] * np.sum(vols)])
plt.title('Cumulative Fraction of Archive')
ax1.set_xlabel('Project in order by size')
plt.tight_layout()
outfile = os.path.join(args.outdir, '.'.join(('cumulative_frac', args.ext)))
plt.savefig(outfile)

# Science category plot
fig, ax1 = plt.subplots()
ax1.set_ylabel('Archive Fraction')
ax1.bar(cat_lookup, cat_totals / np.sum(vols))
ax2 = ax1.twinx()
ax2.set_ylabel('Archive Volume (TB)')
ax2.set_ylim([0, ax1.get_ylim()[1] * np.sum(vols)])
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
plt.tight_layout()
outfile = os.path.join(args.outdir, '.'.join(('volume_per_swg', args.ext)))
plt.savefig(outfile)
