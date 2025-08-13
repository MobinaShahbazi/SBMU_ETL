import re
from rabitpy_dev_phase_info.utils import timing

def _digit_opt_errs(opt):

	try:
		for x in opt:
			if re.search('\s', x['optVal']) is not None:
				x['warning'] = ['1']
			if _not_english(x['optVal']):
				x['warning'] = x.get('warning', [])
				x['warning'].append('0')
	except:
		print('error')

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


def _check_coding_validity(md, nested):

	if nested:
		md['warning'] = md.apply(lambda x: list(), axis=1)
		md.loc[md.duplicated('fldCode', False), 'warning'].apply(lambda x: x.append('3'))
		md.loc[md['opt'].notna(), 'opt'].apply(_digit_opt_errs)
		md.loc[md['fldCode'].apply(_not_english), 'warning'].apply(lambda x: x.append('0'))
		md.loc[md['fldCode'].str.match('^\d'), 'warning'].apply(lambda x: x.append('2'))
		md.loc[md['fldCode'].str.contains('\s'), 'warning'].apply(lambda x: x.append('1'))
	else:
		d = md[['fldCode', 'fldTitle', 'frmCode']].drop_duplicates()
		d['warning'] = d.apply(lambda x: list(), axis=1)
		d.loc[d.duplicated('fldCode', False), 'warning'].apply(lambda x: x.append('3'))
		d.loc[d['fldCode'].apply(_not_english), 'warning'].apply(lambda x: x.append('0'))
		d.loc[d['fldCode'].str.match('^\d'), 'warning'].apply(lambda x: x.append('2'))
		d.loc[d['fldCode'].str.contains('\s'), 'warning'].apply(lambda x: x.append('1'))

		if 'warning' in md.columns:
			md = md.drop(columns='warning')

		md = md.merge(d, on=['fldCode', 'fldTitle', 'frmCode'], how='left')
		del d
		md['optWarn'] = md.apply(lambda x: list(), axis=1)
		md.loc[md['optVal'].notna(), 'optWarn'] = md.loc[md['optVal'].notna(), ['optVal', 'optWarn']]\
			.apply(lambda x: _non_digit_opt_errs(x['optVal'], x['optWarn']), axis=1)

	# TODO: Why do we convert to json again here?
	return md
