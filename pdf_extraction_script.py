import os

j_elem = j_arr[0]
announcements = j_elem['announcement']['Table']
announcement = announcements[0]

pdf_url_format = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/{}"
r = requests.get(pdf_url_format.format(attachement))

failures = {}
def process_announcements(announcements):
	for announcement in announcements:
		try:
			process_announcement(announcement)
		except Exception as e:
			key = str(e)
			failed_list = [] if key not in failures.keys() else failures.get(key)
			failed_list.append(announcement)
			failures[key] = failed_list



def process_announcement(announcement):
	content = announcement_content(announcement)
	if content is not None:
		path = announcement_path(announcement)
		create_dir_for_file_if_not_exists(path)
		f = open(path, 'wb')
		f.write(content)
		f.flush()
		f.close()


def create_dir_for_file_if_not_exists(filename):
	if not os.path.exists(os.path.dirname(filename)):
		try:
			os.makedirs(os.path.dirname(filename))
		except OSError as exc: # Guard against race condition
			if exc.errno != errno.EEXIST:
				raise



def announcement_path(announcement):
	headline = announcement.get("HEADLINE")[:100]
	dt_tm = announcement.get("DT_TM")
	company_id = announcement['SCRIP_CD']
	attachment_name = announcement['ATTACHMENTNAME']
	return "/Users/ashutosh.v/Development/bse_crawling_pdfs/debug/{}/{}_{}_{}".format(company_id, dt_tm, headline, attachment_name)


pdf_url_format = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/{}"
def announcement_content(announcement):
	attachement = announcement.get("ATTACHMENTNAME")
	if attachement is not None:
		r = requests.get(pdf_url_format.format(attachement))
		return r.content
	return None

