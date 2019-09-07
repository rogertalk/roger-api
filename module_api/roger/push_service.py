import logging

from google.appengine.ext import ndb

from roger import config


@ndb.tasklet
def _post_single_async(todo, options):
    if not todo:
        raise RuntimeError('Nothing to do.')
    if config.DEVELOPMENT:
        # TODO: Mock the urlfetch when testing instead.
        logging.debug('Not pushing %d notification(s) to push service (dev)', len(todo))
        for future, _ in todo:
            future.set_result(True)
        return
    bodies = [body for _, body in todo]
    context = ndb.get_context()
    result = yield context.urlfetch(url=config.SERVICE_PUSH,
                                    payload='\n'.join(bodies),
                                    method='POST',
                                    headers={'Content-Type': 'application/json'},
                                    follow_redirects=False,
                                    deadline=60)
    if result.status_code != 200:
        logging.error('Push service returned status %s', result.status_code)
    for future, _ in todo:
        future.set_result(result.status_code == 200)


_batcher = ndb.AutoBatcher(_post_single_async, 500)


def post_async(body):
    return _batcher.add(body, ())
