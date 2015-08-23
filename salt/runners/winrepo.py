# -*- coding: utf-8 -*-
'''
Runner to manage Windows software repo
'''

from __future__ import print_function

from __future__ import absolute_import

# Import python libs
import os

# Import third party libs
import yaml
try:
    import msgpack
except ImportError:
    import msgpack_pure as msgpack

# Import salt libs
import salt.utils
import logging
import salt.minion
from salt.ext.six import string_types

log = logging.getLogger(__name__)


def genrepo():
    '''
    Generate win_repo_cachefile based on sls files in the win_repo

    CLI Example:

    .. code-block:: bash

        salt-run winrepo.genrepo
    '''
    ret = {}
    repo = __opts__['win_repo']
    if not os.path.exists(repo):
        os.makedirs(repo)
    winrepo = __opts__['win_repo_mastercachefile']
    for root, _, files in os.walk(repo):
        for name in files:
            if name.endswith('.sls'):
                with salt.utils.fopen(os.path.join(root, name), 'r') as slsfile:
                    try:
                        config = yaml.safe_load(slsfile.read()) or {}
                    except yaml.parser.ParserError as exc:
                        # log.debug doesn't seem to be working
                        # delete the following print statement
                        # when log.debug works
                        log.debug('Failed to compile'
                                  '{0}: {1}'.format(os.path.join(root, name), exc))
                        __jid_event__.fire_event({'error': 'Failed to compile {0}: {1}'.format(os.path.join(root, name), exc)}, 'progress')
                if config:
                    revmap = {}
                    for pkgname, versions in config.items():
                        for version, repodata in versions.items():
                            if not isinstance(version, string_types):
                                config[pkgname][str(version)] = \
                                    config[pkgname].pop(version)
                            if not isinstance(repodata, dict):
                                log.debug('Failed to compile'
                                          '{0}.'.format(os.path.join(root, name)))
                                __jid_event__.fire_event({'error': 'Failed to compile {0}.'.format(os.path.join(root, name))}, 'progress')
                                continue
                            revmap[repodata['full_name']] = pkgname
                    ret.setdefault('repo', {}).update(config)
                    ret.setdefault('name_map', {}).update(revmap)
    with salt.utils.fopen(os.path.join(repo, winrepo), 'w+b') as repo:
        repo.write(msgpack.dumps(ret))
    return ret


def _extract_key_val(kv, delimiter='='):
    '''Extract key and value from key=val string.
    Example:
    >>> _extract_key_val('foo=bar')
    ('foo', 'bar')
    '''
    pieces = kv.split(delimiter)
    key = pieces[0]
    val = delimiter.join(pieces[1:])
    return key, val

def update_git_repos():
    '''
    Checkout git repos containing Windows Software Package Definitions

    CLI Example:

    .. code-block:: bash

        salt-run winrepo.update_git_repos
    '''
    ret = {}
    mminion = salt.minion.MasterMinion(__opts__)
    repo = __opts__['win_repo']
    gitrepos = __opts__['win_gitrepos']
    repo_cache = __opts__['win_gitrepo_cachedir']

    for gitrepo in gitrepos:
        root = ''
        options = gitrepo.strip().split()
        rev = options[0]
        gitrepo = options[1]
        for extraopt in options[2:]:
            # Support multiple key=val attributes as custom parameters.
            DELIM = '='
            if DELIM not in extraopt:
                log.error('Incorrectly formatted extra parameter. '
                          'Missing \'{0}\': {1}'.format(DELIM, extraopt))
            key, val = _extract_key_val(extraopt, DELIM)
            if key == 'root':
                root = val
            else:
                log.warning('Unrecognized extra parameter: {0}'.format(key))

        if '/' in gitrepo:
            targetname = gitrepo.split('/')[-1]
        else:
            targetname = gitrepo

        gittarget = os.path.join(repo_cache, targetname)
        result = mminion.states['git.latest'](gitrepo,
                                              rev=rev,
                                              target=gittarget,
                                              force=True)
        symlink_path = os.path.join(repo, targetname)
        target_path = gittarget
        if root is not '':
            target_path = os.path.join(target_path, root)
        os.symlink(target_path, symlink_path)

        ret[result['name']] = result['result']
    return ret
