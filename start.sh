#!/usr/bin/env bash

cd $(dirname $0)

[ -d 'venv' ] && venvfolder='venv' || venvfolder='.venv'
if [ -d ${venvfolder} ]
then
  . ${venvfolder}/bin/activate
  python -m tsigma.main
  deactivate
else
  echo "venv folder ${venvfolder} doesn't exist."
fi

