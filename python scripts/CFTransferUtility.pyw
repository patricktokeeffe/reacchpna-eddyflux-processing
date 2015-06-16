# -*- coding: utf-8 -*-
"""Copy TOB3 files from REACCH Obj2 stations to storage location

    *Windows-only : requires system utilities (xcopy, attrib...)*

    Use "--debug" as final argument to enable verbose message output.
"""

import logging
import os
import os.path as osp
import sys
import time
import tempfile

from glob import glob
from subprocess import check_output, CalledProcessError
from threading import Thread
from Queue import Queue

from ConfigParser import RawConfigParser

from Tkinter import *   # analysis:ignore
from ttk import Treeview
from ScrolledText import ScrolledText
from tkFileDialog import askdirectory, askopenfilename
from tkMessageBox import (askokcancel, askyesnocancel, askyesno,
                          showerror, CANCEL)

from win32file import GetDriveType, DRIVE_REMOVABLE

from definitions.fileio import MAX_RAW_FILE_SIZE
from split_toa5 import split_toa5, DEFAULT_MAX_LINES, DEFAULT_HDR_LINES
from standardize_toa5 import standardize_toa5
from version import version as __version__


class CFTransferUtility(Frame):
    """GUI program to transfer files from compactflash card to hard disk"""

    def __init__(self, parent=None, source_dir=None, width=80):
        """Create, display frame"""
        Frame.__init__(self, parent, width=width)
        self.parent = parent

        self._results = {}

        self._srcdir = StringVar() # init later on

        self._destdir_ascii = StringVar()
        self._destdir_stdfmt = StringVar()

        self._do_stdfmt = BooleanVar(value=True)

        self._set_ascii_ro = BooleanVar(value=True)
        self._set_ascii_arc = BooleanVar(value=True)
        self._do_checksums = BooleanVar(value=True)

        self._split_large_files = BooleanVar(value=True)
        self._split_max_size = IntVar(value=(MAX_RAW_FILE_SIZE/1024/1024))
        self._split_num_lines = IntVar(value=DEFAULT_MAX_LINES)
        self._split_delete_sources = BooleanVar(value=True)

        self.log = logging.getLogger(__name__+'.logpane')
        self.log_level = logging.INFO
        if '--debug' in sys.argv:
            self.log_level = logging.DEBUG
        self.log.setLevel(self.log_level)

        self.__gui_setup()
        self.pack(expand=YES, fill=BOTH)

        self._cc_path = self.__find_CardConvert()
        self._read_conf()

        self._num_threads = 8 # TIP modify # of processing threads available

        self.__set_defdir_ascii()
        self.__set_defdir_stdfmt()

        self._srcdir.set(self._defdir_source)
        if source_dir:
            self._srcdir.set(source_dir)
            self.__enable_eject_btn()
            self.search_source_path()

        self.__enable_proc_btn(False)

        #### nasty work-around for split_toa5 logger output
        self.log.write = self.log.info
        sys.stdout = self.log

        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Debug enabled: verbose output will be shown.\n')

    def _read_conf(self):
        """Look for & read configuration file, if present"""
        conf = RawConfigParser(defaults={'source':'',
                                         'output_toa5':'',
                                         #### TODO add archive path
                                         'output_stdfmt':''})
        configfile = osp.splitext(osp.basename(sys.argv[0]))[0] + '.ini'
        PATHS = 'paths'
        SITEMAP = 'site_sn_map'
        conf.read([configfile])

        if conf.has_section(PATHS):
            self._defdir_source = conf.get(PATHS, 'source')
            self._defdir_ascii = conf.get(PATHS, 'output_toa5')
            #### TODO add archive dest path
            self._defdir_stdfmt = conf.get(PATHS, 'output_stdfmt')

        self._site_name_map = {}
        if conf.has_section(SITEMAP):
            for serno in conf.options(SITEMAP):
                sitecode = conf.get(SITEMAP, serno)
                self._site_name_map[str(serno)] = sitecode


    def __gui_setup(self):
        """Put gui widgets onto frame"""
        top = Frame(self)
        top.pack(expand=YES, fill=BOTH)
        app_name = ('Monitoring Tower Data Transfer Utility :: Regional '
                    'Approaches to Climate Change (2011-2016)')
        credit = ('Laboratory for Atmospheric Research at Washington State '
                    'University')
        Label(top, text=app_name).pack(side=TOP)
        Label(top, text=credit).pack(side=TOP)

        frm_opts = self.__gui_options(top)
        frm_opts.pack(side=TOP, expand=YES, fill=X, padx=5)

        vpane = PanedWindow(top, orient=VERTICAL, sashrelief=GROOVE)
        results = self.__gui_profiler(vpane)
        text_log = self.__gui_logpane(vpane)
        vpane.add(results)
        vpane.add(text_log)
        vpane.paneconfigure(results, minsize=20, padx=5, pady=5)
        vpane.paneconfigure(text_log, minsize=20, padx=5, pady=5)
        vpane.pack(side=BOTTOM, expand=YES, fill=BOTH)


    def __gui_options(self, parent=None):
        """construct top pane with entries, buttons, etc"""
        top = Frame(parent)

        #### source frame
        that = LabelFrame(top, padx=5, pady=5, relief=RIDGE,
                          text='Source directory')

        ent_srcdir = Entry(that, textvariable=self._srcdir)
        btn_browsesrc = Button(that, text='Browse',
                               command=self.__set_srcdir)
        btn_refresh = Button(that, text='Refresh',
                             command=self.search_source_path)
        btn_viewsrc = Button(that, text='View',
                             command=self.__view_srcdir)
        btn_viewsrc.pack(side=RIGHT, expand=NO, padx=(5,0))
        btn_refresh.pack(side=RIGHT, expand=NO, padx=(5,0))
        btn_browsesrc.pack(side=RIGHT, expand=NO, padx=(5,0))
        ent_srcdir.pack(side=LEFT, expand=YES, fill=X)

        #### processing options frame
        this = LabelFrame(top, padx=5, pady=5, relief=RIDGE,
                          text='Processing options')

        toa5row = Frame(this)
        lbl_toa5 = Label(toa5row, text='Generate TOA5 files, save in:')
        ent_destascii = Entry(toa5row, textvariable=self._destdir_ascii)
        btn_browseascii = Button(toa5row, text='Browse',
                                 command=self.__set_destdir_ascii)
        btn_defascii = Button(toa5row, text='Default',
                              command=self.__set_defdir_ascii)
        lbl_toa5.pack(side=LEFT, expand=NO)
        btn_defascii.pack(side=RIGHT, expand=NO, padx=(5,0))
        btn_browseascii.pack(side=RIGHT, expand=NO, padx=(5,0))
        ent_destascii.pack(side=LEFT, expand=YES, fill=X)

        asciiopt = Frame(this)
        lbl_asciiopt = Label(asciiopt, text='Set attributes on created files:')
        chb_ascii_ro = Checkbutton(asciiopt, text='read-only',
                                   variable=self._set_ascii_ro)
        chb_ascii_arc = Checkbutton(asciiopt, text='archive',
                                    variable=self._set_ascii_arc)
        lbl_asciiopt.pack(side=LEFT, expand=NO)
        chb_ascii_arc.pack(side=LEFT, expand=NO)
        chb_ascii_ro.pack(side=LEFT, expand=NO)

        md5sumopt = Frame(this)
        chb_md5sum = Checkbutton(md5sumopt,
                                 text=("Calculate md5 checksums of created "
                                       "files, save to `md5sums` (existing "
                                       "is renamed to `md5sums.bak`)"),
                                 variable=self._do_checksums)
        chb_md5sum.pack(side=LEFT, expand=NO)

        ## ensure 'numeric' field entries always contain (only) numbers
        isdigit_validator = (self.register(self.__verify_ent_isdigit), '%P')

        splitopt = Frame(this)
        chb_ascii_split = Checkbutton(splitopt, text='Split files larger than',
                                      variable=self._split_large_files,
                                      command=self.__enable_split_file_opts)
        ent_ascii_size = Entry(splitopt, width = 5,
                               textvariable=self._split_max_size,
                               validate='all',
                               validatecommand=isdigit_validator)
        lbl_ascii_unit = Label(splitopt, text='MB (highlighted in gray) into')
        ent_ascii_lines = Entry(splitopt, width=8,
                                textvariable=self._split_num_lines,
                                validate='all',
                                validatecommand=isdigit_validator)
        lbl_ascii_lines = Label(splitopt, text=' data lines per file ')
        chb_ascii_del = Checkbutton(splitopt,
                                    text='Delete source after splitting',
                                    variable=self._split_delete_sources)
        chb_ascii_split.pack(side=LEFT, expand=NO)
        ent_ascii_size.pack(side=LEFT, expand=NO)
        lbl_ascii_unit.pack(side=LEFT, expand=NO)
        ent_ascii_lines.pack(side=LEFT, expand=NO)
        lbl_ascii_lines.pack(side=LEFT, expand=NO)
        chb_ascii_del.pack(side=LEFT, expand=NO)

        stdfmtrow = Frame(this)
        chb_do_stdfmt = Checkbutton(stdfmtrow, text=('Convert plain-text '
                                    'files to standard format, save in*:'),
                                    variable=self._do_stdfmt,
                                    command=self.__enable_stdfmt_opts)
        ent_deststdfmt = Entry(stdfmtrow, textvariable=self._destdir_stdfmt)
        btn_browsestdfmt = Button(stdfmtrow, text='Browse',
                                  command=self.__set_destdir_stdfmt)
        btn_defstdfmt = Button(stdfmtrow, text='Default',
                               command=self.__set_defdir_stdfmt)
        lbl_dest_hint = Label(stdfmtrow, anchor=W,
                             text=('*files will be sorted into '
                             'subdirectories of this location according '
                             'to corresponding table names'))
        lbl_dest_hint.pack(side=BOTTOM, expand=YES, fill=X, padx=(30,0))
        chb_do_stdfmt.pack(side=LEFT, expand=NO)
        btn_defstdfmt.pack(side=RIGHT, expand=NO, padx=(5,0))
        btn_browsestdfmt.pack(side=RIGHT, expand=NO, padx=(5,0))
        ent_deststdfmt.pack(side=LEFT, expand=YES, fill=X)

        #### text substitution instructions
        subst_hint = Label(this, anchor=W,
                           text=('Tip: the text "%(site)s" in paths will be '
                                 'substituted with the file\'s site code'))

        #### action buttons frame
        btns = Frame(top)
        btn_begin = Button(btns, text='Begin processing',
                           command=self.__begin_processing)
        btn_empty = Button(btns, text='Empty source dir.',
                           command=self.__empty_srcdir)
        btn_eject = Button(btns, text='Eject source dir.',
                           command=self.__eject_srcdir)
        btn_exit = Button(btns, text='Exit',
                          command=self.__exit)
        btn_begin.pack(side=LEFT, expand=NO, padx=5, pady=5)
        btn_exit.pack(side=RIGHT, expand=NO, padx=(5,0), pady=5)
        btn_eject.pack(side=RIGHT, expand=NO, padx=(5,0), pady=5)
        btn_empty.pack(side=RIGHT, expand=NO, padx=(5,0), pady=5)

        #### begin packing
        INDENT = 20
        toa5row.pack(side=TOP, expand=NO, fill=X)
        asciiopt.pack(side=TOP, expand=NO, fill=X, padx=(INDENT,0))
        md5sumopt.pack(side=TOP, expand=NO, fill=X, padx=(INDENT,0))
        splitopt.pack(side=TOP, expand=NO, fill=X, padx=(INDENT,0))
        stdfmtrow.pack(side=TOP, expand=NO, fill=X)
        subst_hint.pack(side=TOP, expand=NO, fill=X, pady=(8,0))

        that.pack(side=TOP, expand=NO, fill=X)
        this.pack(side=TOP, expand=NO, fill=X)
        btns.pack(side=TOP, expand=NO, fill=X, pady=(5,0))

        todisable = []

        self.__ent_srcdir = ent_srcdir
        self.__btn_browsesrc = btn_browsesrc
        self.__btn_refresh = btn_refresh
        self.__btn_viewsrc = btn_viewsrc
        todisable.extend([ent_srcdir, btn_browsesrc, btn_refresh, btn_viewsrc])

        self.__ent_destascii = ent_destascii
        self.__btn_browseascii = btn_browseascii
        self.__btn_defascii = btn_defascii
        self.__chb_ascii_ro = chb_ascii_ro
        self.__chb_ascii_arc = chb_ascii_arc
        self.__chb_md5sum = chb_md5sum
        self.__chb_ascii_split = chb_ascii_split
        self.__ent_ascii_lines = ent_ascii_lines
        self.__ent_ascii_size = ent_ascii_size
        self.__chb_ascii_del = chb_ascii_del
        todisable.extend([ent_destascii, btn_browseascii,
                          btn_defascii, chb_ascii_ro, chb_ascii_arc,
                          chb_md5sum, chb_ascii_split, ent_ascii_lines,
                          ent_ascii_size, chb_ascii_del])

        self.__chb_do_stdfmt = chb_do_stdfmt
        self.__ent_deststdfmt = ent_deststdfmt
        self.__btn_browsestdfmt = btn_browsestdfmt
        self.__btn_defstdfmt = btn_defstdfmt
        todisable.extend([chb_do_stdfmt, ent_deststdfmt, btn_browsestdfmt,
                          btn_defstdfmt])

        self.__btn_begin = btn_begin
        self.__btn_empty = btn_empty
        self.__btn_eject = btn_eject
        self.__btn_exit = btn_exit
        todisable.extend([btn_begin, btn_empty, btn_eject, btn_exit])

        self.__disable_while_processing = todisable
        return top


    def __gui_profiler(self, parent=None):
        """treeview pane for displaying file info"""
        top = LabelFrame(parent, padx=5, pady=5, relief=RIDGE,
                         text='Files found')
        self._resultstree = Treeview(top,selectmode='none',
                        columns=('serno', 'size', 'table', 'tsstr', 'fname'))
        self._resultstree.heading('#0', text='Source file path', anchor=W)
        self._resultstree.heading('serno', text='Serial no.', anchor=W)
        self._resultstree.heading('size', text='File size', anchor=W)
        self._resultstree.heading('table', text='Data table name', anchor=W)
        self._resultstree.heading('tsstr', text='Timestamp', anchor=W)
        self._resultstree.heading('fname', text='TOA5 file name', anchor=W)
        self._resultstree.pack(side=TOP, expand=YES, fill=BOTH)
        return top


    class __ScrolledTextHandler(logging.Handler):
        """bind logger handler to scrolling text object"""
        def __init__(self, widget):
            logging.Handler.__init__(self)
            self.text = widget

        def emit(self, record):
            self.text.configure(state=NORMAL)
            self.text.insert(END, record.msg)
            self.text.see(END)
            self.text.configure(state=DISABLED)


    def __gui_logpane(self, parent):
        """lower pane for processing output"""
        top = LabelFrame(parent, text='Results', padx=5, pady=5)

        self.__logpane = ScrolledText(top, height=10)
        self.__logpane.configure(state=DISABLED)
        self.__logpane.pack(expand=YES, fill=BOTH)

        loghandler = self.__ScrolledTextHandler(self.__logpane)
        loghandler.setLevel(self.log_level)
        self.log.addHandler(loghandler)

        return top

    ##########################
    #### GUI ^ / handlers v
    ##########################

    def __find_CardConvert(self):
        """Search local system for CardConvert"""
        guess = osp.join(os.environ['PROGRAMFILES'],
                         r'Campbellsci\CardConvert\CardConvert.exe')
        if osp.isfile(guess):
            return guess
        guess = r'C:\Program Files\Campbellsci\CardConvert\CardConvert.exe'
        if osp.isfile(guess):
            return guess
        guess = r'C:\Program Files (x86)\Campbellsci\CardConvert\CardConvert.exe'
        if osp.isfile(guess):
            return guess
        msg = ('Could not locate the CardConvert executable (CardConvert.exe).'
                '\nPress "Yes" to browse for the file or "No" to exit.')
        choice = askyesno(message=msg, title='CardConvert not found')
        if choice:
            guess = askopenfilename(title='Locate CardConvert',
                                    filetypes=[('Applications', '*.exe'),
                                               ('All file types', '*.*')],
                                    initialdir=os.environ['PROGRAMFILES']
                                    )
            if guess and osp.isfile(guess):
                return guess
        self.__exit(quickly=True)


    def __set_srcdir(self):
        """browse to source directory"""
        choice = askdirectory(title='Select source directory',
                              mustexist=True,
                              initialdir=self._srcdir.get())
        if choice and osp.isdir(choice):
            choice = osp.normpath(choice)
            self._srcdir.set(choice)
            self.__enable_eject_btn()
        self.search_source_path()


    def __enable_eject_btn(self):
        """if source drive is removable, enable eject button"""
        state = DISABLED
        srcdir = self._srcdir.get()
        if osp.isdir(srcdir) and self._is_removable_media(srcdir):
            state = NORMAL
        self.__btn_eject.configure(state=state)


    def __enable_proc_btn(self, enabled):
        """if files are found & characterized, enable processing"""
        state = NORMAL if enabled else DISABLED
        self.__btn_begin.config(state=state)


    def __enable_split_file_opts(self):
        """if splitting files enable associated options"""
        state = NORMAL if self._split_large_files.get() else DISABLED
        self.__ent_ascii_size.config(state=state)
        self.__ent_ascii_lines.config(state=state)
        self.__chb_ascii_del.config(state=state)
        self.__chb_do_stdfmt.config(state=state)
        self.__ent_deststdfmt.config(state=state)
        self.__refresh_profiler() # b/c of oversized file highlighting


    def __enable_stdfmt_opts(self):
        """if re-writing to std format, enable associated options"""
        state = NORMAL if self._do_stdfmt.get() else DISABLED
        self.__ent_deststdfmt.config(state=state)
        self.__btn_browsestdfmt.config(state=state)
        self.__btn_defstdfmt.config(state=state)


    def __set_destdir_ascii(self):
        self.__set_destdir(var=self._destdir_ascii, name='plain-text')
    def __set_destdir_stdfmt(self):
        self.__set_destdir(var=self._destdir_stdfmt, name='standard format')


    def __set_destdir(self, var, name):
        choice = askdirectory(initialdir=var.get(),
                    title='Select destination for %s data files' % name)
        if choice and not osp.isfile(choice):
            choice = osp.normpath(choice)
            var.set(choice)


    def __set_defdir_ascii(self):
        self._destdir_ascii.set(self._defdir_ascii)
    def __set_defdir_stdfmt(self):
        self._destdir_stdfmt.set(self._defdir_stdfmt)


    def __view_srcdir(self):
        dirpath = self._srcdir.get()
        if not dirpath:
            return
        try:
            cmd = 'start explorer %s' % dirpath
            os.system(cmd)
        except:
            # usually caught by Windows Explorer; just in case...
            msg = 'An error occurred while attempting to view %s'
            showerror(title='View error', message=(msg % dirpath))


    def __verify_ent_isdigit(self, newval):
        """ensure input to max file size field is integer"""
        try:
            self.after(25, self.__refresh_profiler())
        except:
            pass
        return newval.isdigit()


    def search_source_path(self):
        srcdir = self._srcdir.get()
        if not osp.isdir(srcdir):
            self.log.info('! Invalid source directory: %s\n' % srcdir)
            flist = []
        else:
            flist = self.list_files_in(srcdir)
        self.__profile_files(flist)
        valid_files = self.__refresh_profiler()
        btn_state = True if valid_files else False
        self.__enable_proc_btn(btn_state)


    def __refresh_profiler(self):
        """reconstruct results pane treeview"""
        w = self._resultstree
        split = self._split_large_files.get()
        maxsize = self._split_max_size.get() * 1024 # scale MB -> KB
        valid_files = []
        for row in w.get_children():
            w.delete(row)
        for (fpath, meta) in sorted(self._results.items()):
            (serno, fsize, table, cdate, fname) = (meta['serno'],
                                                   meta['size'],
                                                   meta['table'],
                                                   meta['tsstr'],
                                                   meta['fname'])
            tags = []
            if split and (fsize > maxsize):
                tags.append('+size')
            if not table:
                tags.append('invalid')
            else:
                valid_files.append(fpath)
            w.insert('', END, iid=fpath, text=fpath, tags=tags,
                     values=[serno, str(fsize)+' KB', table, cdate, "TOA5_"+fname])
        self._resultstree.tag_configure('+size', background='lightgray')
        self._resultstree.tag_configure('invalid', background='pink')
        return valid_files


    def __set_GUI_lock(self, isLocked):
        """(dis/en)able interactive elements of GUI during processing"""
        state = DISABLED if isLocked else NORMAL
        for widget in self.__disable_while_processing:
            # (un)lock all widgets with a `state` property
            if 'state' in widget.config():
                widget.config(state=state)
        if not isLocked: # if all were unlocked, selectively re-disable
            self.__enable_ascii_opts()
            self.__enable_split_file_opts()
            self.__enable_stdfmt_opts()
            self.__enable_eject_btn()


    ##########################
    #### handlers ^ / logic v
    ##########################


    def list_files_in(self, in_dir):
        """return list of TOB3-formatted files in source directory """
        if not in_dir or not osp.isdir(in_dir):
            return []

        # OK since TOB3 are created with '.dat' ending
        flist = glob(osp.join(in_dir, '*.dat'))
        return sorted(flist)

    def serno2code(self, serno):
        """Attempt to look up site code from serial no"""
        return self._site_name_map.get(serno, serno)

    class _FileFormatError(Exception): pass

    def __extract_tob3_metadata(self, tob3):
        """extract metadata from file"""
        with open(tob3, mode='rb') as f:
            line1 = f.readline().strip()
            line2 = f.readline().strip()
        try:
            hdr1 = [s.strip('"') for s in line1.split(',')]
            hdr2 = [s.strip('"') for s in line2.split(',')]
            ftype, _, _, serno, _, _, _, tsstr = hdr1
            table = hdr2[0]
        except ValueError as err: # most likely trigger is tuple unpacking
            msg = 'Could not unpack TOB3 header: %s (%s)' % (err, tob3)
            raise self._FileFormatError(msg)
        if ftype != 'TOB3':
            msg = 'Invalid file format: %s' % ftype
            raise self._FileFormatError(msg)
        site = self.serno2code(serno)
        return {'serno':serno, 'tsstr':tsstr, 'table':table, 'site':site}


    def __create_asciidest_fname(self, site, table, tstamp):
        """CardConvert prepends files with 'TOA5_'; rename before converting"""
        ts = tstamp.replace('-','').replace(':','').replace(' ','.')[:-2]
        return '%s_%s_%s.dat' % (site, ts, table)


    #### standardize_toa5 handles it's own output file names


    def __profile_files(self, flist):
        """build descriptions of tob3 files"""
        self._results = {}
        for fname in flist:
            try:
                fsize = osp.getsize(fname) / 1024
                meta = self.__extract_tob3_metadata(fname)
            except IOError as err:
                self.log.info('! Could not profile file: %s\n' % str(err))
                continue
            except (self._FileFormatError) as err:
                self._results[fname] = {'serno': str(err), 'size':fsize,
                                'table':'', 'tsstr':'', 'site':'', 'fname':''}
                continue
            toa5name = self.__create_asciidest_fname(meta['site'],
                                                     meta['table'],
                                                     meta['tsstr'])
            self._results[fname] = {'serno' : meta['serno'],
                                    'size' : fsize,
                                    'table' : meta['table'],
                                    'tsstr' : meta['tsstr'],
                                    'site' : meta['site'],
                                    'fname' : toa5name}


    def __begin_processing(self):
        """process data files"""
        self.log.debug('Entering processing routine \n')
        do_stdfmt = self._do_stdfmt.get()
        split_files = self._split_large_files.get()
        num_threads = self._num_threads

        self.__set_GUI_lock(True)
        ccQ, splitQ, stdQ = Queue(), Queue(), Queue()

        # segregate by do/don't process and source site
        bysite = {}
        for (fname, meta) in sorted(self._results.items()):
            if not meta['fname']:
                self.log.info('! Skipping %s (%s) \n' % (fname, meta['serno']))
                continue
            asite = bysite.setdefault(meta['site'], {})
            asite[fname] = meta

        # move to-process files into per-site temp dirs
        orig_names = {}
        to_process = {}
        tmp_dirs = {}
        for (site, files) in bysite.items():
            self.log.info('* Moving %s files into temp. directory \n' % site)
            tmpdir = tempfile.mkdtemp(dir=self._srcdir.get())
            tmp_dirs[site] = tmpdir
            for origname, meta in files.items():
                newname = osp.join(tmpdir, meta['fname'])
                os.rename(origname, newname)
                orig_names[newname] = origname
                to_process[newname] = meta

        try:
            # convert binary into ascii files
            self.log.info('\nStarting binary to plain-text conversion \n')
            set_ro = self._set_ascii_ro.get()
            set_arc = self._set_ascii_arc.get()
            do_md5 = self._do_checksums.get()
            existing_files = {}
            for i in range(num_threads):
                T = self._ThreadedCC(self, ccQ)
                T.setDaemon(True)
                T.start()
            for (site, dir_) in tmp_dirs.items():
                source = dir_
                target = self._destdir_ascii.get() % {'site' : site}
                existing_files[target] = set(self.list_files_in(target))
                ccQ.put( (source, target, set_ro, set_arc, do_md5) )
            while ccQ.unfinished_tasks:
                self.parent.update_idletasks()
                time.sleep(0.5)

            # locate newly-made TOA5 files
            if split_files:
                self.log.info('\nStarting oversized file split routine \n')
                maxsize = self._split_max_size.get()*1024*1024 # MB -> bytes
                num_lines = self._split_num_lines.get()
                hdr_lines = DEFAULT_HDR_LINES
                del_src = self._split_delete_sources.get()
                for i in range(num_threads):
                    T = self._ThreadedSplitFile(self, splitQ)
                    T.setDaemon(True)
                    T.start()
                for target, fileset in existing_files.items():
                    new_files = list(set(self.list_files_in(target)) - fileset)
                    to_split = [f for f in new_files if osp.getsize(f) > maxsize]
                    if not to_split:
                        self.log.info('~ No oversized files found in %s \n' %
                                        target)
                    for ea in to_split:
                        splitQ.put( (ea, num_lines, hdr_lines, del_src) )
                while splitQ.unfinished_tasks:
                    self.parent.update_idletasks()
                    time.sleep(0.5)

                if do_stdfmt:
                    self.log.info('\nStarting standardizing routine \n')
                    for src, files in existing_files.items():
                        self.log.debug('Source directory: %s \n' % src)
                        to_stdize = list(set(self.list_files_in(src))-files)
                        for ea in sorted(to_stdize):
                            if osp.getsize(ea) > maxsize:
                                self.log.info('~ Skipping oversized file %s ...' % ea)
                                continue
                            destpath = osp.join(self._destdir_stdfmt.get(),
                                                '%(table)s')
                            self.log.info('. Standardizing %s ... ' % ea)
                            self.update()
                            standardize_toa5(ea, dest_path=destpath, baled=True)
                            self.log.info('done \n')
                    ## No, the standardizing process is not threaded because
                    ## of the very high potential for I/O conflicts as time-series
                    ## files are written & re-written.

        except Exception as err:
            self.log.error('='*80+'\n! Caught exception: %s\n'%err+'='*80+'\n')

        finally:
            # restore original files names & clean-up
            self.log.info('\nRelocating files to original directory(s)\n')
            for newname, oldname in orig_names.items():
                os.rename(newname, oldname)
            for _, dir_ in tmp_dirs.items():
                os.removedirs(dir_)

            self.__set_GUI_lock(False)


    def _set_readonly_attr(self, fname, active=True):
        """Set file's read-only attribute true"""
        self.log.debug('Entered `set_readonly_attr` targeting %s \n' % fname)
        try:
            msg = '%setting read-only flag: %%s ... ' % ('+ S' if active
                                                         else '- Uns')
            cmd = 'attrib %sR "%%s"' % ('+' if active else '-')
            rc = check_output(cmd % fname, shell=True)
            self.log.info( (msg % fname) + rc.strip() + '\n')
            return True
        except CalledProcessError as err:
            self.log.error('! Failed to %sset read-only (%s): %s \n' %
                            (('' if active else 'un'), fname, err))
            return False


    def _set_archive_attr(self, fname, active=True):
        """Set file's archive attribute true"""
        self.log.debug('Entered `set_archive_attr` targeting %s \n' % fname)
        try:
            msg = '%setting archive   flag: %%s ... ' % ('+ S' if active
                                                       else '- Uns')
            cmd = 'attrib %sA "%%s"' % ('+' if active else '-')
            rc = check_output(cmd % fname, shell=True)
            self.log.info( (msg % fname) + rc.strip() + '\n')
            return True
        except CalledProcessError as err:
            self.log.error('! Failed to %sset archive flag (%s): %s \n' %
                            (('' if active else 'un'), fname, err))
            return False


    def _checksum_ascii(self, filelist):
        self.log.debug('Entered `checksum_ascii` with %d files' % len(filelist))
        try:
            cmd = 'generate_md5sums.py ' + ' '.join(filelist)
            rc = check_output(cmd, shell=True)
            self.log.info('\n%s\n' % rc.strip())
            return True
        except CalledProcessError as err:
            self.log.error('! Failed to create checksum file: %s \n' % err)
            return False


    class _ThreadedCC(Thread):
        def __init__(self, parent, queue):
            Thread.__init__(self)
            self.parent = parent
            self.q = queue
        def run(self):
            while True:
                (source, target, RO, ARCH, MD5) = self.q.get()
                self.parent._launch_cardconvert(source, target, RO, ARCH, MD5)
                self.q.task_done()


    def _launch_cardconvert(self, source, target, set_ro=False, set_arc=False,
                            do_md5sums=False):
        self.log.debug('Entering binary file conversion routine \n')

        orig_files = set(self.list_files_in(target))

        # TODO handle need to overwriting existing files
        self._CardConvert(source, target)

        new_files = list(set(self.list_files_in(target)) - orig_files)
        for ea in new_files:
            if set_ro:
                self._set_readonly_attr(ea)
            if set_arc:
                self._set_archive_attr(ea)
        if do_md5sums:
            self._checksum_ascii(new_files)


    def _CardConvert(self, source_dir, target_dir):
        """Launch CardConvert"""
        ccpath = self._cc_path
        if not osp.isfile(ccpath):
            self.log.error('! CardConvert is no longer available! Aborting'
                           'binary file conversion')
            return False

        try: # CC chokes if destination doesn't exist yet...
            os.makedirs(target_dir)
        except OSError:
            if not osp.isdir(target_dir):
                raise

        cmd = '"' + ccpath + '" runfile="%(ccf)s"'
        template = """\
[main]
SourceDir=%(source)s\\
TargetDir=%(target)s\\
Format=0
FileMarks=0
RemoveMarks=0
RecNums=1
Timestamps=1
CreateNew=1
DateTimeNames=0
ColWidth=493
ListHeight=366
ListWidth=190
BaleCheck=0
DOY=0
Append=0
ConvertNew=0
CSVOptions=6619599
BaleStart=40179
BaleInterval=32875
"""
        ccf = tempfile.NamedTemporaryFile(suffix='.ccf', delete=False)
        ccf.write(template % {'source' : source_dir, 'target' : target_dir})
        ccf.close()
        try:
            rc = check_output(cmd % {'ccf' : ccf.name})
        except CalledProcessError as err:
            self.log.error('! CardConvert encountered an error: %s\n' % err)
            return False
        try:
            os.remove(ccf.name)
        except (IOError, OSError):
            self.log.warn('# Could not remove temp file containing '
                          'CardConvert settings\n')
        return rc


    class _ThreadedSplitFile(Thread):
        def __init__(self, parent, queue):
            Thread.__init__(self)
            self.parent = parent
            self.q = queue
        def run(self):
            while True:
                (target, lines, hdr, delete) = self.q.get()
                self.parent._split_file(target, lines, hdr, delete)
                self.q.task_done()


    def _split_file(self, to_split, max_lines, hdr_lines, del_src):
        # must remove RO attribute done within plain-text conversion step
        self._set_readonly_attr(to_split, active=False)
        new_files = split_toa5(to_split, max_lines=max_lines,
                               hdr_lines=hdr_lines, delete_source=del_src)
        for ea in new_files:
            self._set_readonly_attr(ea)


    def __eject_srcdir(self):
        """if possible eject source directory"""
        # TODO
        print 'entered __eject_srcdir'


    def __empty_srcdir(self, no_confirm=False):
        """delete all data files in source directory"""
        self.log.info('Emptying source directory (%s)\n' % self._srcdir.get())
        flist = self.list_files_in(self._srcdir.get())
        if not flist:
            self.log.info('No source files to delete \n')
            return # ignore if empty

        if not no_confirm:
            msg = 'Delete all %s TOB3 data files in source directory?'
            if not askyesno(title='Delete confirm', message=msg % len(flist)):
                self.log.info('Aborted. Source directory not emptied \n')
                return # abort if 'No'

        for fname in flist:
            try:
                os.remove(fname)
                self.log.info('Removed %s \n' % fname)
            except OSError:
                self.log.error('Failed to remove %s \n' % fname)


    def __exit(self, quickly=False):
        """confirm then exit application"""
        srcdir = self._srcdir.get()
        confirmed = True if quickly else None

        # attempt to determine if source directory is a CF card and if so,
        # prompt to delete any remaining files before exit
        if not quickly and srcdir and osp.isdir(srcdir):
            drive, path = osp.splitdrive(srcdir.rstrip('\\').rstrip('/'))
            if not path and self._is_removable_media(srcdir):
                flist = self.list_files_in(self._srcdir.get())
                if flist:
                    msg = ('Would you like to permanently delete all %s TOB3 '
                          'data files in the source directory before exit?' %
                          len(flist))
                    clear = askyesnocancel(title='Clear card?', message=msg,
                                           default=CANCEL)
                    confirmed = False if clear is None else True
                    if clear:
                        self.log.info('Exit task: removing files from source '
                                      'directory (%s) \n' % srcdir)
                        self.__empty_srcdir(no_confirm=True)

        if confirmed is None:
            confirmed = askokcancel(title='Confirm', message='Really exit?')

        if confirmed: self.parent.destroy()


    def _is_removable_media(self, path):
        """determine if path is on removable media; not fool-proof"""
        drive, _ = osp.splitdrive(path)
        is_removable = False
        try:
            is_removable = (GetDriveType(drive) != DRIVE_REMOVABLE)
        except OSError:
            pass
        return is_removable


if __name__ == '__main__':
    # Don't allow to use network location as CWD (SOOO SLOW)
    if os.getcwd().startswith('\\\\'):
        os.chdir('C:\\')

    root = Tk()
    root.title('CF Transfer Utility | WSU LAR')
    if len(sys.argv) > 1:
        CFTransferUtility(root,
                          source_dir=sys.argv[1]).mainloop()
    else:
        CFTransferUtility(root).mainloop()
