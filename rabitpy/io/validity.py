import pandas as pd
import json
import re


def _digit_opt_errs(opt):
	for x in opt:
		if re.search('\s', x['value']) is not None:
			x['warning'] = ['1']
		if _not_english(x['value']):
			x['warning'] = x.get('warning', [])
			x['warning'].append('0')
	return opt


def _non_digit_opt_errs(optVal, optWarn):
	if re.search('\s', optVal) is not None:
		optWarn.append('1')
	if _not_english(optVal):
		optWarn.append('0')
	return optWarn


def _not_english(s):
	try:
		str(s).encode(encoding='utf-8').decode('ascii')
	except UnicodeDecodeError:
		return True
	else:
		return False


def _check_coding_validity(metadata, nested):
	df = pd.DataFrame(metadata)

	if nested:
		df['warning'] = df.apply(lambda x: list(), axis=1)
		df.loc[df.duplicated('fldCode', False), 'warning'].apply(lambda x: x.append('3'))
		df.loc[df['opt'].notna(), 'opt'].apply(_digit_opt_errs)
		df.loc[df['fldCode'].apply(_not_english), 'warning'].apply(lambda x: x.append('0'))
		df.loc[df['fldCode'].str.match('^\d'), 'warning'].apply(lambda x: x.append('2'))
		df.loc[df['fldCode'].str.contains('\s'), 'warning'].apply(lambda x: x.append('1'))
	else:
		d = df[['fldCode', 'fldTitle', 'frmCode']].drop_duplicates()
		d['warning'] = d.apply(lambda x: list(), axis=1)
		d.loc[d.duplicated('fldCode', False), 'warning'].apply(lambda x: x.append('3'))
		d.loc[d['fldCode'].apply(_not_english), 'warning'].apply(lambda x: x.append('0'))
		d.loc[d['fldCode'].str.match('^\d'), 'warning'].apply(lambda x: x.append('2'))
		d.loc[d['fldCode'].str.contains('\s'), 'warning'].apply(lambda x: x.append('1'))

		if 'warning' in df.columns:
			df = df.drop(columns='warning')

		df = df.merge(d, on=['fldCode', 'fldTitle', 'frmCode'], how='left')
		del d
		df['optWarn'] = df.apply(lambda x: list(), axis=1)
		df.loc[df['optVal'].notna(), 'optWarn'] = df.loc[df['optVal'].notna(), ['optVal', 'optWarn']]\
			.apply(lambda x: _non_digit_opt_errs(x['optVal'], x['optWarn']), axis=1)

	metadata = df.to_json(orient='records')
	del df

	# TODO: Why do we convert to json again here?
	return json.loads(metadata)
