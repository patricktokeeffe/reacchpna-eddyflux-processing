# -*- coding: utf-8 -*-
"""
Created on Mon Dec 09 14:57:26 2013

@author: pokeeffe
"""

import os
import os.path as osp
import logging

from sys import argv
from datetime import datetime
from glob import glob

from Tkinter import *
from tkFileDialog import askdirectory, asksaveasfilename
from tkMessageBox import showerror
from ScrolledText import ScrolledText
from ttk import Treeview

try:
    # Homepage: https://github.com/ianare/exif-py
    from exifread import process_file as get_exif_tags
except ImportError:
    Tk().withdraw() # hide default window
    msg = ('The module `exifread` is missing. Please install by running "pip '
            'install exifread" \nas an Administrator. The source can be found'
            ' at https://github.com/ianare/exif-py. \n\nPress OK to exit.')
    showerror(title='Missing module', message=msg)
    exit()

from PIL import Image, ImageTk

from win32file import GetDriveType, DRIVE_REMOVABLE

from definitions.sites import site_list
from definitions.paths import TIMELAPSE_PHOTOS
from version import version as __version__


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


class SDTransferUtility(Frame):
    """GUI program for transferring timelapse images from SD cards"""

    def __init__(self, parent=None, search_dir=None, search_str=None):
        Frame.__init__(self, parent)
        self.pack(padx=5, pady=5, expand=YES, fill=BOTH)

        self._prog_title = 'Timelapse Image Transfer Utility'
        self._sources = {}
        self._log_output = IntVar(value=0)
        self._log_fname = StringVar(value='')
        self._search_dir = StringVar()
        self._search_str = StringVar()
        self._preview_img = None

        if search_dir: self._search_dir.set(search_dir)
        if search_str: self._search_str.set(search_str)

        self.master.title(self._prog_title)
        self.__gui_setup()
        if self._search_dir.get():
            self.__enable_eject_btn()
            if self._search_str.get():
                self.__search()


    def __gui_setup(self):
        """Build up GUI from widgets"""
        topfrm = Frame(self)
        topfrm.pack(expand=YES, fill=BOTH, side=TOP)
        Label(topfrm, text=self._prog_title).pack(side=TOP)

        out_vpane = PanedWindow(topfrm, orient=VERTICAL, sashrelief=GROOVE)
        btm_hpane = PanedWindow(out_vpane, orient=HORIZONTAL, sashrelief=GROOVE)
        top_hpane = PanedWindow(out_vpane, orient=HORIZONTAL, sashrelief=GROOVE)
        inn_vpane = PanedWindow(top_hpane, orient=VERTICAL, sashrelief=GROOVE)

        out_vpane.pack(side=TOP, fill=BOTH, expand=YES, padx=5, pady=5)
        btm_hpane.pack(side=TOP, fill=BOTH, expand=YES, padx=5, pady=5)
        top_hpane.pack(side=TOP, fill=BOTH, expand=YES, padx=5, pady=5)
        inn_vpane.pack(side=TOP, fill=BOTH, expand=YES, padx=5, pady=5)

        win_search = self.__gui_search(inn_vpane)
        win_results = self.__gui_results(inn_vpane)
        win_preview = self.__gui_preview(top_hpane)
        win_logger = self.__gui_logger(btm_hpane)
        win_buttons = self.__gui_buttons(btm_hpane)

        out_vpane.add(btm_hpane)
        out_vpane.add(top_hpane, before=btm_hpane)
        top_hpane.add(inn_vpane)
        inn_vpane.add(win_search)
        inn_vpane.add(win_results)
        top_hpane.add(win_preview)
        btm_hpane.add(win_logger)
        btm_hpane.add(win_buttons)

        out_vpane.paneconfigure(top_hpane, minsize=100)
        out_vpane.paneconfigure(btm_hpane, minsize=100)
        top_hpane.paneconfigure(inn_vpane)
        inn_vpane.paneconfigure(win_search, padx=5, pady=5, minsize=50)
        inn_vpane.paneconfigure(win_results, padx=5, pady=5, minsize=50)
        top_hpane.paneconfigure(win_preview, padx=5, pady=5, minsize=300)
        btm_hpane.paneconfigure(win_logger, padx=5, pady=5, minsize=300)
        btm_hpane.paneconfigure(win_buttons, padx=5, pady=5, minsize=100)


    def __gui_search(self, parent=None):
        """Upper pane for entry fields, buttons, so on """
        thispane = LabelFrame(parent, padx=5, pady=5, relief=RIDGE,
                              text='Source Image Search')

        row1 = Frame(thispane)
        lbl_dir = Label(row1, text='Root search dir.:')
        ent_dir = Entry(row1, textvariable=self._search_dir)
        btn_dir = Button(row1, text='Browse', command=self.__set_search_dir)

        row2 = Frame(thispane)
        lbl_find = Label(row2, text='Match pattern:')
        ent_find = Entry(row2, textvariable=self._search_str)
        btn_find = Button(row2, text='Search', command=self.__search)

        lbl_dir.pack(side=LEFT)
        btn_dir.pack(side=RIGHT, padx=(5,0))
        ent_dir.pack(side=LEFT, expand=YES, fill=X)
        row1.pack(side=TOP, expand=NO, fill=X, pady=(0,5))

        lbl_find.pack(side=LEFT)
        btn_find.pack(side=RIGHT, padx=(5,0))
        ent_find.pack(side=LEFT, expand=YES, fill=X)
        row2.pack(side=TOP, expand=NO, fill=X)

        return thispane


    def __gui_results(self, parent=None):
        """Window with tree of files found sorted by directory"""
        thispane = LabelFrame(parent, padx=5, pady=5, relief=RIDGE,
                              text='Search Results')
        lbl = Label(thispane, anchor=W,
                    text='Right-click directory name, then select source site')
        lbl.pack(side=TOP, expand=NO, fill=X, pady=(0,5))
        self._sourcetree = Treeview(thispane,
                                    columns=('destname'),
                                    selectmode='browse')
        self._sourcetree.heading('destname', text='Destination', anchor=W)
        self._sourcetree.pack(side=TOP, expand=YES, fill=BOTH)
        return thispane


    def __gui_preview(self, parent=None):
        """Middle pane interacting with user"""
        thispane = LabelFrame(parent, padx=5, pady=5, relief=RIDGE,
                              text='Image Preview')
        self._preview = Button(thispane)
        self._preview.pack(side=TOP, expand=YES, fill=BOTH)
        return thispane


    class __TextLogger(logging.Handler):
        """Tie logger into Tkinter Text object"""
        def __init__(self, widget):
            logging.Handler.__init__(self)
            self.text = widget

        def emit(self, record):
            self.text.insert(END, record.msg + '\n')
            self.text.see(END)


    def __gui_logger(self, parent=None):
        """Lower pane with logging output"""
        thispane = LabelFrame(parent, padx=5, pady=5, relief=RIDGE,
                              text='Logging')

        hfrm = Frame(thispane)
        chb_logging = Checkbutton(hfrm, text='Log output to: ',
                                  variable=self._log_output)
        ent_logpath = Entry(hfrm, textvariable=self._log_fname)
        btn_browse = Button(hfrm, text='Browse', command=self.__set_logfile)
        chb_logging.pack(expand=NO, fill=X, side=LEFT)
        ent_logpath.pack(expand=YES, fill=X, side=LEFT)
        btn_browse.pack(expand=NO, padx=(5,0), side=RIGHT)
#        hfrm.pack(expand=NO, fill=X, side=BOTTOM, pady=(5,0))

        self.__logpane = ScrolledText(thispane, height=12)
        self.__logpane.pack(expand=YES, fill=BOTH, side=BOTTOM)

        ## tie into logging
        log2pane = self.__TextLogger(self.__logpane)
        log2pane.setLevel(logging.INFO)
        logger.addHandler(log2pane)

        return thispane


    def __gui_buttons(self, parent=None):
        """Lower-right pane containing action buttons"""
        thispane = Frame(parent)

        go_btn = Button(thispane, text='Begin processing',
                        command=self.__transfer_images,
                        state=DISABLED)
        eject_btn = Button(thispane, text='Eject source dir.',
                           command=self.__eject_srch_dir)
        quit_btn = Button(thispane, text='Exit program',
                          command=self.__quit)
        go_btn.pack(side=TOP)
        quit_btn.pack(side=BOTTOM)
        eject_btn.pack(side=BOTTOM, pady=(0, 5))

        self.__begin_proc_btn = go_btn
        self.__eject_src_btn = eject_btn
        return thispane


    def __gui_popup(self, event):
        """Pop-up context menu for selecting site"""
        w = self._sourcetree
        row = w.identify_row(event.y)
        menu = Menu(tearoff=0)
        for site in site_list:
            def make_caller(iid, code):
                return lambda: self.__set_srcdir_site(iid=iid, code=code)
            site = site.code
            menu.add_command(label=site, command=make_caller(row, site))
        menu.post(event.x_root, event.y_root)


    ##### GUI ^ / LOGIC v #####

    def __set_search_dir(self):
        """Browse to source directory"""
        oldchoice = self._search_dir.get()
        choice = askdirectory(title='Select source directory',
                              mustexist=True)
        if choice and osp.isdir(choice):
            choice = osp.normpath(choice)
            self._search_dir.set(choice)
            self.__enable_eject_btn()
            if choice != oldchoice:
                self._sources.clear()
                self.__refresh_treeview()


    def __set_srcdir_site(self, iid, code):
        """set key from None to site's code"""
        srcdir = self._sourcetree.item(iid, option='text')
        destdir = TIMELAPSE_PHOTOS % {'site' : code}
        self._sources[srcdir]['dest_dir'] = destdir
        self._sources[srcdir]['site_code'] = code

        self.__refresh_treeview()
        self.__enable_processing()


    def __set_logfile(self):
        """Browse to target log file"""
        fname = asksaveasfilename(title='Log to file', parent=self)
        if fname:
            self._log_fname.set(fname)


    def __enable_eject_btn(self):
        """if the source drive is removable, enable the 'eject' button"""
        state = DISABLED
        srcdir = self._search_dir.get()
        if osp.isdir(srcdir):
            drive, _ = osp.splitdrive(srcdir)
            if GetDriveType(drive) == DRIVE_REMOVABLE:
                logger.info('Source directory drive type is DRIVE_REMOVABLE so '
                             'enabling eject button')
                state = NORMAL
        self.__eject_src_btn.configure(state=state)


    def __enable_processing(self):
        """if conditions are OK, enable the 'begin processing' button"""
        state = NORMAL
        for _, info in self._sources.items():
            if not info['dest_dir']:
                state = DISABLED
            if not info['site_code']:
                state = DISABLED
        self.__begin_proc_btn.configure(state=state)


    def __search(self):
        """Search for files in source directory"""
        globstr = osp.join(self._search_dir.get(), self._search_str.get())
        files_found = glob(globstr)
        self._sources.clear()
        for f in files_found:
            this_dir = self._sources.setdefault(osp.dirname(f), {})
            this_dir.setdefault('dest_dir', None) # not used
            this_dir.setdefault('site_code', None) # just set defaults
            dest_names = this_dir.setdefault('dest_names', {})
            dest_names[f] = None # init to none
        self.__refresh_treeview()


    def __refresh_treeview(self):
        """Construct tree view data model"""
        w = self._sourcetree

        # remember open tree controls & current selection
        selected_row = w.selection()
        open_nodes = []
        open_opt = BooleanVar() # for coercing _tkinter objects to bool
        for row in w.get_children():
            open_opt.set(str(w.item(row, option='open'))) # force to bool
            if open_opt.get():
                open_nodes.append(row)
            w.delete(row)

        # populate
        for src_dir in sorted(self._sources.keys()):
            dest_dir = self._sources[src_dir]['dest_dir']
            dest_names = self._sources[src_dir]['dest_names']
            site_code = self._sources[src_dir]['site_code']

            dest_str = dest_dir or '<not yet determined>'
            w.insert('', END, iid=src_dir, text=src_dir,
                     tag='dir', values=[dest_str])
            for src_name in sorted(dest_names.keys()):
                dest_name = self.__dest_fname_mask(src_name)
                if site_code:
                    dest_name = dest_name % {'code' : site_code}
                    dest_names[src_name] = dest_name
                w.insert(src_dir, END, text=osp.basename(src_name), tag='img',
                         iid=src_name, values=[dest_name])
        w.tag_bind('dir', sequence='<Button-3>', callback=self.__gui_popup)
        w.bind('<<TreeviewSelect>>', lambda event: self.__preview_img())

        # restore open tree controls & select previous item
        topchildren = w.get_children()
        for row in open_nodes:
            if row in topchildren:
                w.item(row, open=True)
        if selected_row and w.exists(selected_row):
            w.selection_set(selected_row)


    def __preview_img(self):
        """Calculate size of, then display image"""
        # event not used
        w = self._sourcetree
        if w.focus():
            fname = w.item(w.focus(), option='text')
            if fname in self._sources.keys():
                if self._preview_img:
                    self._preview.configure(text='', image=None)
                    self._preview_img = None
            else:
                srcdir = w.item(w.parent(w.focus()), option='text')
                imgpath = osp.join(srcdir, fname)
                try:
                    img = Image.open(imgpath)
                    wd = self._preview.winfo_width() # button dimensions
                    ht = self._preview.winfo_height() - 25 # text label space
                    img.thumbnail((wd,ht), Image.ANTIALIAS)
                    self._preview_img = ImageTk.PhotoImage(img)
                    self._preview.configure(text=imgpath,
                                            image=self._preview_img,
                                            compound=TOP)
                except Exception as ex:
                    self._preview.configure(text='<Preview not available>',
                                            image=None)
                    self._preview_img = None
                    logger.debug('Exception prevented image preview load:\n'
                                 + ex.message)


    def __dest_fname_mask(self, fname):
        """Return image destination name file mask

        In form of `%(site)s_YYYYMMDD.hhmm` YYYYMMDD is year/month/day, hhmm
        is (24) hour/min, and %(site)s is for dict-style string substitution.
        """
        _, ext = osp.splitext(fname)
        tags = get_exif_tags(open(fname, mode='rb'),
                             details=False,
                             stop_tag='DateTimeOriginal')
        timestamp = str(tags['EXIF DateTimeOriginal'])
        dt = datetime.strptime(timestamp, '%Y:%m:%d %H:%M:%S')
        return dt.strftime('%%(code)s_%Y%m%d.%H%M'+ext.lower())


    def __transfer_images(self):
        """Process image files from results objects"""
        if self._preview_img:
            self._preview.configure(text='', image=None) # de-associate
            self._preview_img = None # then release image file

        dirs_to_remove = []
        for srcdir, info in sorted(self._sources.items()):
            dest_names = info['dest_names']
            if not dest_names:
                continue
            dest_dir = info['dest_dir']
            try:
                os.makedirs(dest_dir)
            except OSError as e:
                if not osp.isdir(dest_dir):
                    raise e
            files_to_remove = []
            for src_path, dest_file in sorted(dest_names.items()):
                dest_path = osp.join(dest_dir, dest_file)
                moved = self.__move_image(src_path, dest_path)
                if moved:
                    files_to_remove.append(src_path)
            for ea in files_to_remove:
                dest_names.pop(ea) # remove file name if moved
            if not dest_names:
                dirs_to_remove.append(srcdir)
        for ea in dirs_to_remove:
            self._sources.pop(ea, None) # remove dirs with no files left

        self.__refresh_treeview()


    def __move_image(self, src, dst):
        """Move single image; threadable"""
        try:
            os.rename(src, dst)
            logger.info('Moved %s to %s' % (src, dst))
            return True
        except WindowsError as err:
            logger.info('Error moving %s (file skipped):  %s' %
                        (src, err.strerror))
            return False


    def __eject_srch_dir(self):
        """attempt to eject source directory"""
        to_eject = self._search_dir.get()
        if not to_eject or not osp.isdir(to_eject):
            return
        drive, _ = osp.splitdrive(to_eject)
        if GetDriveType(drive) != DRIVE_REMOVABLE:
            logger.info('NOT A REMOVABLE DRIVE!')
            return
        if not osp.isfile('usb_disk_eject.exe'):
            logger.info('CANNOT FIND DISK EJECTING SOFTWARE!')
            return
        try:
            driveletter = drive.strip(':')
            cwd = osp.dirname(argv[0])
            eject = osp.join('"'+cwd, 'usb_disk_eject.exe" /REMOVELETTER %s')
            os.system(eject % driveletter)
            logger.info('SUCCESS EJECTING DISK!')
        except Exception as err:
            logger.info('WAS NOT ABLE TO EXIT! Exception:\n' + err.message)


    def __quit(self):
        """exit program"""
        # reserved for clean-up
        self.master.destroy()


if __name__=='__main__':
    if len(argv) > 2:
        SDTransferUtility(search_dir=argv[1], search_str=argv[2]).mainloop()
    elif len(argv) > 1:
        SDTransferUtility(search_dir=argv[1]).mainloop()
    else:
        SDTransferUtility().mainloop()

