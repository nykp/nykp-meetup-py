from typing import List, Optional, Tuple

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

from nykp_meetup.utils.secrets import get_meetup_auth_token

MISSING_NAME = "MISSING"


def get_guest_name(member_name: str) -> str:
    return f"GUEST ({member_name})"


def _get_client(retries=10) -> Client:
    transport = RequestsHTTPTransport(
        url="https://api.meetup.com/gql",
        headers={"Content-Type": "application/json",
                 "Authorization": get_meetup_auth_token()},
        verify=True,
        retries=retries,
    )
    return Client(transport=transport, fetch_schema_from_transport=False)


def _get_events_attendees_query(group_name: str, cursor: Optional[str]) -> str:
    if cursor:
        cursor_spec = f'after: "{cursor}"'
    else:
        cursor_spec = ""
    query_template = """
        query {{
        groupByUrlname(urlname: "{group_name}") {{
            id
            name
            link
            pastEvents(input: {{{cursor_spec}}}) {{
                pageInfo {{
                    hasNextPage
                    hasPreviousPage
                    startCursor
                    endCursor
                }}
                count
                edges {{
                    cursor
                    node {{
                        id
                        title
                        dateTime
                        status
                        going
                        tickets {{
                            count
                            edges {{
                                node {{
                                    user {{
                                        id
                                        name
                                        city
                                        state
                                    }}
                                    status
                                    guestsCount
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
    }}
    """
    return query_template.format(group_name=group_name, cursor_spec=cursor_spec)


def _get_events_attendees_page(group_name: str, client, cursor) -> Tuple[List[dict], Optional[str]]:
    def get_cursor_end(r: dict) -> Optional[str]:
        page_info = r['groupByUrlname']['pastEvents']['pageInfo']
        if page_info['hasNextPage']:
            return page_info['endCursor']
    
    def get_event_attendees(r: dict) -> List[dict]:
        edges = r['groupByUrlname']['pastEvents'].get('edges', [])
        attendees = []
        for edge in edges:
            event = edge['node']
            event['cursor'] = edge['cursor']
            event_attendees = get_attendees(event)
            attendees.extend(event_attendees)
        return attendees
    
    def get_attendees(r: dict) -> List[dict]:
        r['event_id'] = r.pop('id')
        r['event_status'] = r.pop('status')
        edges = r.pop('tickets').get('edges', [])
        attendees = []
        for edge in edges:
            rsvp = edge['node']
            if rsvp['status'] in ('YES', 'ATTENDED'):
                attendee = rsvp.copy()
                attendee.update(attendee.pop('user'))
                attendee['user_id'] = attendee.pop('id')
                attendee['attend_status'] = attendee.pop('status')
                attendee.update(r)
                attendees.append(attendee)
                if attendee['guestsCount'] > 0:
                    guest = r.copy()
                    guest.update({'name': get_guest_name(attendee['name']),
                                  'attend_status': attendee['attend_status']})
                    attendees.append(guest)
        if len(attendees) < r['going']:
            unidentified = r.copy()
            unidentified.update({'name': MISSING_NAME, 'attend_status': 'YES'})
            for _ in range(r['going'] - len(attendees)):
                attendees.append(unidentified.copy())
        return attendees
    
    q_str = _get_events_attendees_query(group_name, cursor)
    query = gql(q_str)
    result = client.execute(query)
    attendees = get_event_attendees(result)
    cursor_end = get_cursor_end(result)
    return attendees, cursor_end


def get_all_events_attendees(
    group_name: str, client=None, cursor=None, page_limit=None, progress_pages=10
) -> Tuple[List[dict], Optional[str]]:
    client = client or _get_client()
    pages = 0
    attendees, cursor = _get_events_attendees_page(group_name, client, cursor)
    pages += 1
    if (pages % progress_pages) == 0:
        print(f"Pulled {len(attendees)} attendees so far, up to {attendees[-1]['dateTime']}")
    while cursor and (page_limit is None or pages < page_limit):
        next_attendees, cursor = _get_events_attendees_page(group_name, client, cursor)
        attendees.extend(next_attendees)
        pages += 1
        if (pages % progress_pages) == 0:
            print(f"Pulled {len(attendees)} attendees so far, up to {attendees[-1]['dateTime']}"
                  f" (last cursor: '{attendees[-1]['cursor']}')")
    return attendees
