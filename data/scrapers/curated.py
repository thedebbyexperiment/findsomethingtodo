from __future__ import annotations

import logging
import re
from datetime import datetime

from normalize.schema import (
    Activity,
    DataType,
    ExperienceType,
    ParentParticipation,
    TimeSlot,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


def _slugify(name: str) -> str:
    """Convert a venue name to a URL-friendly slug."""
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


def _time_slot_from_str(time_str: str) -> list[TimeSlot]:
    """Derive TimeSlot list from a human-readable time string."""
    if not time_str:
        return []
    t = time_str.lower()
    # Extract the hour from patterns like "10:30am", "2pm", "6:30pm"
    match = re.search(r"(\d{1,2})(?::\d{2})?\s*(am|pm)", t)
    if match:
        hour = int(match.group(1))
        ampm = match.group(2)
        if ampm == "pm" and hour != 12:
            hour += 12
        if ampm == "am" and hour == 12:
            hour = 0
        if hour < 12:
            return [TimeSlot.morning]
        if hour < 17:
            return [TimeSlot.afternoon]
        return [TimeSlot.evening]
    # No parseable time — default to morning + afternoon
    return [TimeSlot.morning, TimeSlot.afternoon]


# ---------------------------------------------------------------------------
# Curated venue data
# ---------------------------------------------------------------------------

CURATED_VENUES: list[dict] = [
    # -----------------------------------------------------------------------
    # Museums with family programming
    # -----------------------------------------------------------------------
    {
        "name": "Whitney Museum of American Art",
        "address": "99 Gansevoort St, New York, NY 10014",
        "lat": 40.7396,
        "lng": -74.0089,
        "url": "https://whitney.org",
        "category": "Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Open Studio for Kids",
                "day": "Saturdays",
                "time": "10:30am",
                "description": "Free with admission. Drop-in art-making for families in the Whitney's education studios.",
                "price_display": "Free with admission",
            },
        ],
    },
    {
        "name": "Metropolitan Museum of Art",
        "address": "1000 Fifth Ave, New York, NY 10028",
        "lat": 40.7794,
        "lng": -73.9632,
        "url": "https://www.metmuseum.org",
        "category": "Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Met Kids Programs",
                "day": "Weekends",
                "time": "10:00am",
                "description": "Gallery activities, sketching, storytelling, and art-making for kids and families.",
                "price_display": "Free with admission",
            },
            {
                "name": "Family Audio Guide",
                "day": "Daily",
                "time": "",
                "description": "Self-guided family audio tour through highlights of the collection, designed for kids 6-12.",
                "price_display": "Free with admission",
            },
        ],
    },
    {
        "name": "Museum of Modern Art (MoMA)",
        "address": "11 W 53rd St, New York, NY 10019",
        "lat": 40.7614,
        "lng": -73.9776,
        "url": "https://www.moma.org",
        "category": "Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Family Gallery Talks",
                "day": "Weekends",
                "time": "11:00am",
                "description": "Educator-led conversations in the galleries designed for families with children ages 4-12.",
                "price_display": "Free with admission",
            },
        ],
    },
    {
        "name": "Brooklyn Museum",
        "address": "200 Eastern Pkwy, Brooklyn, NY 11238",
        "lat": 40.6712,
        "lng": -73.9636,
        "url": "https://www.brooklynmuseum.org",
        "category": "Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Target First Saturdays",
                "day": "First Saturday of month",
                "time": "5:00pm",
                "description": "Free admission evening with live music, dancing, films, and family-friendly activities.",
                "price_display": "Free",
            },
        ],
    },
    {
        "name": "American Museum of Natural History",
        "address": "200 Central Park West, New York, NY 10024",
        "lat": 40.7813,
        "lng": -73.9740,
        "url": "https://www.amnh.org",
        "category": "Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Discovery Room",
                "day": "Daily",
                "time": "10:00am",
                "description": "Hands-on exhibits where kids can explore specimens, puzzles, and scientific tools.",
                "price_display": "Free with admission",
            },
        ],
    },
    {
        "name": "New York Hall of Science",
        "address": "47-01 111th St, Queens, NY 11368",
        "lat": 40.7472,
        "lng": -73.8518,
        "url": "https://nysci.org",
        "category": "Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Great Hall Programs",
                "day": "Weekends",
                "time": "11:00am",
                "description": "Interactive science demos and workshops for families in the Great Hall.",
                "price_display": "Free with admission",
            },
        ],
    },
    {
        "name": "Brooklyn Children's Museum",
        "address": "145 Brooklyn Ave, Brooklyn, NY 11213",
        "lat": 40.6745,
        "lng": -73.9440,
        "url": "https://www.brooklynkids.org",
        "category": "Children's Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 10,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Children's Museum of Manhattan",
        "address": "212 W 83rd St, New York, NY 10024",
        "lat": 40.7848,
        "lng": -73.9741,
        "url": "https://cmom.org",
        "category": "Children's Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 8,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Staten Island Children's Museum",
        "address": "1000 Richmond Terrace, Staten Island, NY 10301",
        "lat": 40.6433,
        "lng": -74.0986,
        "url": "https://www.sichildrensmuseum.org",
        "category": "Children's Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 10,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Bronx Children's Museum",
        "address": "725 Exterior St, Bronx, NY 10451",
        "lat": 40.8260,
        "lng": -73.9283,
        "url": "https://www.bronxchildrensmuseum.org",
        "category": "Children's Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 10,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Intrepid Sea, Air & Space Museum",
        "address": "Pier 86, W 46th St, New York, NY 10036",
        "lat": 40.7645,
        "lng": -73.9997,
        "url": "https://www.intrepidmuseum.org",
        "category": "Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Kids Week Programs",
                "day": "School breaks",
                "time": "10:00am",
                "description": "Special hands-on STEM activities and tours for kids during school vacation weeks.",
                "price_display": "Free with admission",
            },
        ],
    },
    {
        "name": "New York Transit Museum",
        "address": "99 Schermerhorn St, Brooklyn, NY 11201",
        "lat": 40.6903,
        "lng": -73.9900,
        "url": "https://www.nytransitmuseum.org",
        "category": "Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Museum of Mathematics (MoMath)",
        "address": "11 E 26th St, New York, NY 10010",
        "lat": 40.7445,
        "lng": -73.9878,
        "url": "https://momath.org",
        "category": "Museum",
        "experience_type": ExperienceType.educational,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Family Fridays",
                "day": "Fridays",
                "time": "6:30pm",
                "description": "Evening math explorations for the whole family with hands-on puzzles and activities.",
                "price_display": "Included with admission",
            },
        ],
    },
    # -----------------------------------------------------------------------
    # Performance venues with kids shows
    # -----------------------------------------------------------------------
    {
        "name": "New Victory Theater",
        "address": "209 W 42nd St, New York, NY 10036",
        "lat": 40.7569,
        "lng": -73.9881,
        "url": "https://newvictory.org",
        "category": "Theater",
        "experience_type": ExperienceType.performance,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Family Programming",
                "day": "Year-round",
                "time": "",
                "description": "NYC's premier theater for kids and families with shows spanning theater, dance, puppetry, and circus arts.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Lincoln Center",
        "address": "10 Lincoln Center Plaza, New York, NY 10023",
        "lat": 40.7725,
        "lng": -73.9835,
        "url": "https://www.lincolncenter.org",
        "category": "Performing Arts",
        "experience_type": ExperienceType.performance,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "LC Kids",
                "day": "Weekends",
                "time": "11:00am",
                "description": "Family-friendly performances including concerts, dance, and theater for young audiences.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Symphony Space",
        "address": "2537 Broadway, New York, NY 10025",
        "lat": 40.8032,
        "lng": -73.9682,
        "url": "https://www.symphonyspace.org",
        "category": "Performing Arts",
        "experience_type": ExperienceType.performance,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Just Kidding Series",
                "day": "Weekends",
                "time": "11:00am",
                "description": "Music, storytelling, dance, and theater performances curated for kids and families.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "BAM (Brooklyn Academy of Music)",
        "address": "30 Lafayette Ave, Brooklyn, NY 11217",
        "lat": 40.6862,
        "lng": -73.9782,
        "url": "https://www.bam.org",
        "category": "Performing Arts",
        "experience_type": ExperienceType.performance,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "BAMkids Film Festival",
                "day": "Annual (winter)",
                "time": "10:00am",
                "description": "Annual film festival featuring curated shorts and features for young audiences.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Carnegie Hall",
        "address": "881 7th Ave, New York, NY 10019",
        "lat": 40.7651,
        "lng": -73.9799,
        "url": "https://www.carnegiehall.org",
        "category": "Performing Arts",
        "experience_type": ExperienceType.performance,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Family Concerts",
                "day": "Select weekends",
                "time": "2:00pm",
                "description": "Interactive concerts designed to introduce kids to classical, jazz, and world music.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "92nd Street Y",
        "address": "1395 Lexington Ave, New York, NY 10128",
        "lat": 40.7847,
        "lng": -73.9534,
        "url": "https://www.92ny.org",
        "category": "Community Center",
        "experience_type": ExperienceType.performance,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Kids Performances and Classes",
                "day": "Weekends",
                "time": "10:00am",
                "description": "Live performances, music, dance, and art classes for babies through age 12.",
                "price_display": "Varies",
            },
        ],
    },
    # -----------------------------------------------------------------------
    # Activity venues (drop-in / one-off)
    # -----------------------------------------------------------------------
    {
        "name": "Unity Jiu Jitsu",
        "address": "229 W 28th St, New York, NY 10001",
        "lat": 40.7483,
        "lng": -73.9937,
        "url": "https://www.unityjiujitsu.com",
        "category": "Martial Arts",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Open Kids Time",
                "day": "Saturdays",
                "time": "11:00am",
                "description": "Open mat time for kids to practice jiu jitsu in a supervised, fun environment.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "KidsAtWork NYC",
        "address": "244 W 54th St, New York, NY 10019",
        "lat": 40.7637,
        "lng": -73.9831,
        "url": "https://www.kidsatworknyc.com",
        "category": "Indoor Play",
        "experience_type": ExperienceType.active,
        "age_min": 0,
        "age_max": 6,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Drop-in Play and Classes",
                "day": "Daily",
                "time": "9:00am",
                "description": "Open play sessions and structured classes for babies and toddlers.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Brooklyn Boulders",
        "address": "575 Degraw St, Brooklyn, NY 11217",
        "lat": 40.6809,
        "lng": -73.9794,
        "url": "https://brooklynboulders.com",
        "category": "Climbing",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Kids Climbing Programs",
                "day": "Weekends",
                "time": "10:00am",
                "description": "Youth climbing sessions and clubs for beginner to advanced young climbers.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Chelsea Piers",
        "address": "62 Chelsea Piers, New York, NY 10011",
        "lat": 40.7468,
        "lng": -74.0083,
        "url": "https://www.chelseapiers.com",
        "category": "Sports Complex",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Open Gym, Sports & Skating",
                "day": "Daily",
                "time": "10:00am",
                "description": "Drop-in open gym, ice skating, rock climbing, and sports programs for kids.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Aviator Sports",
        "address": "3159 Flatbush Ave, Brooklyn, NY 11234",
        "lat": 40.5911,
        "lng": -73.8907,
        "url": "https://www.aviatorsports.com",
        "category": "Sports Complex",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Ice Skating, Rock Climbing & Gymnastics",
                "day": "Daily",
                "time": "10:00am",
                "description": "Drop-in ice skating, rock climbing wall, and gymnastics open sessions for kids.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "BOUNCE Trampoline Park",
        "address": "1250 Broadway, New York, NY 10001",
        "lat": 40.7489,
        "lng": -73.9883,
        "url": "https://www.bounceinc.com",
        "category": "Trampoline Park",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Open Bounce",
                "day": "Daily",
                "time": "10:00am",
                "description": "Open jump sessions on trampolines, foam pits, and obstacle courses.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "City Ice Pavilion",
        "address": "47-32 32nd Pl, Long Island City, NY 11101",
        "lat": 40.7430,
        "lng": -73.9310,
        "url": "https://www.cityicepavilion.com",
        "category": "Ice Skating",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Public Skating Sessions",
                "day": "Daily",
                "time": "",
                "description": "Public ice skating sessions with skate rentals available.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Sky Zone",
        "address": "140-32 Jamaica Ave, Jamaica, NY 11435",
        "lat": 40.7040,
        "lng": -73.8032,
        "url": "https://www.skyzone.com",
        "category": "Trampoline Park",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Open Jump Sessions",
                "day": "Daily",
                "time": "10:00am",
                "description": "Open jump time on trampolines, dodgeball, and foam zones.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Asphalt Green",
        "address": "555 E 90th St, New York, NY 10128",
        "lat": 40.7789,
        "lng": -73.9436,
        "url": "https://www.asphaltgreen.org",
        "category": "Sports & Swim",
        "experience_type": ExperienceType.active,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Family Swim & Open Gym",
                "day": "Weekends",
                "time": "10:00am",
                "description": "Family swim sessions and open gym time for kids and parents.",
                "price_display": "Varies",
            },
        ],
    },
    # -----------------------------------------------------------------------
    # Bookstores with kids events
    # -----------------------------------------------------------------------
    {
        "name": "Books of Wonder",
        "address": "42 W 17th St, New York, NY 10011",
        "lat": 40.7387,
        "lng": -73.9936,
        "url": "https://www.booksofwonder.com",
        "category": "Bookstore",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Saturday Storytime",
                "day": "Saturdays",
                "time": "11:00am",
                "description": "Free storytime with author readings and activities for kids.",
                "price_display": "Free",
            },
        ],
    },
    {
        "name": "McNally Jackson",
        "address": "52 Prince St, New York, NY 10012",
        "lat": 40.7234,
        "lng": -73.9944,
        "url": "https://www.mcnallyjackson.com",
        "category": "Bookstore",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Kids Events",
                "day": "Varies",
                "time": "",
                "description": "Author readings, book launches, and storytime events for young readers.",
                "price_display": "Free",
            },
        ],
    },
    {
        "name": "Books Are Magic",
        "address": "225 Smith St, Brooklyn, NY 11231",
        "lat": 40.6836,
        "lng": -73.9931,
        "url": "https://www.booksaremagic.net",
        "category": "Bookstore",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Storytime and Kids Events",
                "day": "Weekends",
                "time": "11:00am",
                "description": "Regular storytime sessions and author events for kids and families.",
                "price_display": "Free",
            },
        ],
    },
    {
        "name": "The Strand",
        "address": "828 Broadway, New York, NY 10003",
        "lat": 40.7334,
        "lng": -73.9909,
        "url": "https://www.strandbooks.com",
        "category": "Bookstore",
        "experience_type": ExperienceType.educational,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Kids Events",
                "day": "Varies",
                "time": "",
                "description": "Author readings, book signings, and family-friendly events in the kids section.",
                "price_display": "Free",
            },
        ],
    },
    # -----------------------------------------------------------------------
    # Family movie screenings
    # -----------------------------------------------------------------------
    {
        "name": "AMC Theatres",
        "address": "234 W 42nd St, New York, NY 10036",
        "lat": 40.7570,
        "lng": -73.9888,
        "url": "https://www.amctheatres.com",
        "category": "Movie Theater",
        "experience_type": ExperienceType.performance,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "AMC Sensory Friendly Films",
                "day": "Select Saturdays",
                "time": "10:00am",
                "description": "Screenings with lights turned up, sound turned down, and kids free to move and talk. Designed for families with children on the autism spectrum but open to everyone.",
                "price_display": "Regular ticket price",
            },
        ],
    },
    {
        "name": "Alamo Drafthouse Cinema",
        "address": "445 Albee Square W, Brooklyn, NY 11201",
        "lat": 40.6887,
        "lng": -73.9826,
        "url": "https://drafthouse.com",
        "category": "Movie Theater",
        "experience_type": ExperienceType.performance,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Alamo Kids Camp",
                "day": "Select weekends & school breaks",
                "time": "10:00am",
                "description": "Family-friendly classic and new film screenings with themed activities and affordable pricing.",
                "price_display": "$5",
            },
        ],
    },
    {
        "name": "Nitehawk Cinema",
        "address": "136 Metropolitan Ave, Brooklyn, NY 11249",
        "lat": 40.7144,
        "lng": -73.9622,
        "url": "https://nitehawkcinema.com",
        "category": "Movie Theater",
        "experience_type": ExperienceType.performance,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Lil' Hawk Family Matinees",
                "day": "Weekends",
                "time": "11:00am",
                "description": "Weekend morning screenings of family-friendly films in a relaxed environment where kids can be kids.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Film Forum",
        "address": "209 W Houston St, New York, NY 10014",
        "lat": 40.7278,
        "lng": -74.0002,
        "url": "https://filmforum.org",
        "category": "Movie Theater",
        "experience_type": ExperienceType.performance,
        "age_min": 5,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Film Forum Jr.",
                "day": "Saturdays",
                "time": "11:00am",
                "description": "Curated classic and international films for young audiences followed by discussions.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "CinemaKidz",
        "address": "123-10 14th Ave, College Point, NY 11356",
        "lat": 40.7852,
        "lng": -73.8418,
        "url": "https://www.cinemakidz.com",
        "category": "Movie Theater",
        "experience_type": ExperienceType.performance,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Kids Movie Screenings",
                "day": "Weekends",
                "time": "10:00am",
                "description": "Affordable kids-only movie screenings in a dedicated family cinema.",
                "price_display": "Varies",
            },
        ],
    },
    # -----------------------------------------------------------------------
    # Bowling, mini golf, and family fun
    # -----------------------------------------------------------------------
    {
        "name": "Brooklyn Bowl",
        "address": "61 Wythe Ave, Brooklyn, NY 11249",
        "lat": 40.7221,
        "lng": -73.9577,
        "url": "https://www.brooklynbowl.com",
        "category": "Bowling",
        "experience_type": ExperienceType.active,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Family Bowl",
                "day": "Sundays",
                "time": "12:00pm",
                "description": "Family-friendly bowling with bumpers, lighter balls, and kid-friendly food. Live music sometimes.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Bowlero Chelsea Piers",
        "address": "60 Chelsea Piers, New York, NY 10011",
        "lat": 40.7472,
        "lng": -74.0078,
        "url": "https://www.bowlero.com",
        "category": "Bowling",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Family Bowling",
                "day": "Weekends",
                "time": "10:00am",
                "description": "Family bowling with bumper lanes, arcade games, and birthday party packages.",
                "price_display": "Varies",
            },
        ],
    },
    {
        "name": "Frames Bowling Lounge",
        "address": "550 9th Ave, New York, NY 10018",
        "lat": 40.7546,
        "lng": -73.9952,
        "url": "https://framesnyc.com",
        "category": "Bowling",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Melody Lanes",
        "address": "461 37th St, Brooklyn, NY 11232",
        "lat": 40.6549,
        "lng": -73.9931,
        "url": "https://www.melodylanes.com",
        "category": "Bowling",
        "experience_type": ExperienceType.active,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Bowlmor Times Square",
        "address": "222 W 44th St, New York, NY 10036",
        "lat": 40.7578,
        "lng": -73.9874,
        "url": "https://www.bowlmor.com",
        "category": "Bowling",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Pier 25 Mini Golf",
        "address": "Pier 25, N Moore St, New York, NY 10013",
        "lat": 40.7203,
        "lng": -74.0127,
        "url": "https://hudsonriverpark.org/activities/mini-golf",
        "category": "Mini Golf",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": False,
        "recurring_programs": [
            {
                "name": "Mini Golf at Pier 25",
                "day": "Daily (seasonal)",
                "time": "10:00am",
                "description": "18-hole mini golf course along the Hudson River with NYC skyline views. Open spring through fall.",
                "price_display": "$5-$10",
            },
        ],
    },
    {
        "name": "Shipwrecked Mini Golf",
        "address": "621 Court St, Brooklyn, NY 11231",
        "lat": 40.6755,
        "lng": -73.9979,
        "url": "https://shipwreckednyc.com",
        "category": "Mini Golf",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Indoor Mini Golf",
                "day": "Daily",
                "time": "10:00am",
                "description": "Pirate-themed indoor mini golf course with 18 holes, arcade games, and party rooms.",
                "price_display": "$10-$15",
            },
        ],
    },
    {
        "name": "Putting Green at Randall's Island",
        "address": "20 Randalls Island Park, New York, NY 10035",
        "lat": 40.7935,
        "lng": -73.9219,
        "url": "https://randallsisland.org",
        "category": "Mini Golf",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": False,
        "recurring_programs": [],
    },
    {
        "name": "Dave & Buster's (Times Square)",
        "address": "234 W 42nd St, New York, NY 10036",
        "lat": 40.7570,
        "lng": -73.9888,
        "url": "https://www.daveandbusters.com",
        "category": "Arcade & Entertainment",
        "experience_type": ExperienceType.active,
        "age_min": 3,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Coney Island Luna Park",
        "address": "1000 Surf Ave, Brooklyn, NY 11224",
        "lat": 40.5735,
        "lng": -73.9790,
        "url": "https://lunaparknyc.com",
        "category": "Amusement Park",
        "experience_type": ExperienceType.active,
        "age_min": 0,
        "age_max": 12,
        "indoor": False,
        "recurring_programs": [],
    },
    {
        "name": "Victorian Gardens (Central Park)",
        "address": "830 5th Ave, New York, NY 10065",
        "lat": 40.7679,
        "lng": -73.9718,
        "url": "https://www.victoriangardens.com",
        "category": "Amusement Park",
        "experience_type": ExperienceType.active,
        "age_min": 0,
        "age_max": 10,
        "indoor": False,
        "recurring_programs": [],
    },
    {
        "name": "American Dream (Nickelodeon Universe)",
        "address": "1 American Dream Way, East Rutherford, NJ 07073",
        "lat": 40.8124,
        "lng": -74.0709,
        "url": "https://www.americandream.com",
        "category": "Theme Park & Entertainment",
        "experience_type": ExperienceType.active,
        "age_min": 0,
        "age_max": 12,
        "indoor": True,
        "recurring_programs": [
            {
                "name": "Nickelodeon Universe Theme Park",
                "day": "Daily",
                "time": "10:00am",
                "description": "Indoor theme park with rides, games, and Nickelodeon character meet-and-greets. Also has DreamWorks water park and LEGOLAND Discovery Center.",
                "price_display": "$49+",
            },
        ],
    },
    {
        "name": "LEGOLAND Discovery Center",
        "address": "1 American Dream Way, East Rutherford, NJ 07073",
        "lat": 40.8124,
        "lng": -74.0709,
        "url": "https://www.legolanddiscoverycenter.com/new-jersey",
        "category": "Indoor Play",
        "experience_type": ExperienceType.creative,
        "age_min": 3,
        "age_max": 10,
        "indoor": True,
        "recurring_programs": [],
    },
    # -----------------------------------------------------------------------
    # Playgrounds
    # -----------------------------------------------------------------------
    {
        "name": "Seravalli Playground",
        "address": "Horatio St & Greenwich St, New York, NY 10014",
        "lat": 40.7356,
        "lng": -74.0063,
        "url": "https://www.nycgovparks.org/parks/corporal-john-a-seravalli-playground",
        "category": "Playground",
        "experience_type": ExperienceType.active,
        "age_min": 0,
        "age_max": 12,
        "indoor": False,
        "recurring_programs": [],
    },
    {
        "name": "Bleecker Street Playground",
        "address": "Bleecker St & W 11th St, New York, NY 10014",
        "lat": 40.7337,
        "lng": -74.0002,
        "url": "https://www.nycgovparks.org/parks/bleecker-playground",
        "category": "Playground",
        "experience_type": ExperienceType.active,
        "age_min": 0,
        "age_max": 12,
        "indoor": False,
        "recurring_programs": [],
    },
    {
        "name": "James J. Walker Park",
        "address": "Hudson St & Clarkson St, New York, NY 10014",
        "lat": 40.7268,
        "lng": -74.0064,
        "url": "https://www.nycgovparks.org/parks/james-j-walker-park",
        "category": "Park",
        "experience_type": ExperienceType.active,
        "age_min": 0,
        "age_max": 12,
        "indoor": False,
        "recurring_programs": [],
    },
    # -----------------------------------------------------------------------
    # Event centers (venues only — events come from Ticketmaster)
    # -----------------------------------------------------------------------
    {
        "name": "Javits Center",
        "address": "429 11th Ave, New York, NY 10001",
        "lat": 40.7576,
        "lng": -74.0023,
        "url": "https://javitscenter.com",
        "category": "Event Center",
        "experience_type": ExperienceType.events,
        "age_min": 0,
        "age_max": 18,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Barclays Center",
        "address": "620 Atlantic Ave, Brooklyn, NY 11217",
        "lat": 40.6826,
        "lng": -73.9754,
        "url": "https://www.barclayscenter.com",
        "category": "Event Center",
        "experience_type": ExperienceType.events,
        "age_min": 0,
        "age_max": 18,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Madison Square Garden",
        "address": "4 Pennsylvania Plaza, New York, NY 10001",
        "lat": 40.7505,
        "lng": -73.9934,
        "url": "https://www.msg.com",
        "category": "Event Center",
        "experience_type": ExperienceType.events,
        "age_min": 0,
        "age_max": 18,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Prudential Center",
        "address": "25 Lafayette St, Newark, NJ 07102",
        "lat": 40.7334,
        "lng": -74.1711,
        "url": "https://www.prucenter.com",
        "category": "Event Center",
        "experience_type": ExperienceType.events,
        "age_min": 0,
        "age_max": 18,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "UBS Arena",
        "address": "2400 Hempstead Tpke, Elmont, NY 11003",
        "lat": 40.7157,
        "lng": -73.7271,
        "url": "https://www.ubsarena.com",
        "category": "Event Center",
        "experience_type": ExperienceType.events,
        "age_min": 0,
        "age_max": 18,
        "indoor": True,
        "recurring_programs": [],
    },
    {
        "name": "Citi Field",
        "address": "41 Seaver Way, Queens, NY 11368",
        "lat": 40.7571,
        "lng": -73.8458,
        "url": "https://www.mlb.com/mets/ballpark",
        "category": "Event Center",
        "experience_type": ExperienceType.events,
        "age_min": 0,
        "age_max": 18,
        "indoor": False,
        "recurring_programs": [],
    },
    {
        "name": "Yankee Stadium",
        "address": "1 E 161st St, Bronx, NY 10451",
        "lat": 40.8296,
        "lng": -73.9262,
        "url": "https://www.mlb.com/yankees/ballpark",
        "category": "Event Center",
        "experience_type": ExperienceType.events,
        "age_min": 0,
        "age_max": 18,
        "indoor": False,
        "recurring_programs": [],
    },
    {
        "name": "MetLife Stadium",
        "address": "1 MetLife Stadium Dr, East Rutherford, NJ 07073",
        "lat": 40.8128,
        "lng": -74.0742,
        "url": "https://www.metlifestadium.com",
        "category": "Event Center",
        "experience_type": ExperienceType.events,
        "age_min": 0,
        "age_max": 18,
        "indoor": False,
        "recurring_programs": [],
    },
]


class CuratedScraper(BaseScraper):
    """Static/curated seed list of known family-friendly venues and recurring programs in NYC.

    This scraper makes no HTTP calls. It returns a hand-maintained list of
    venues and their known recurring family programming, which can be enriched
    over time. Each venue produces a ``data_type=venue`` Activity, and each
    recurring program produces a ``data_type=event`` Activity.
    """

    source_name: str = "curated"

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------

    def fetch_raw(self) -> list[dict]:
        """Return the curated venue list directly (no network calls)."""
        logger.info("Returning %d curated venues", len(CURATED_VENUES))
        return list(CURATED_VENUES)

    def normalize(self, raw_items: list[dict]) -> list[Activity]:
        """Convert curated venue dicts into Activity objects.

        Each venue becomes a ``data_type=venue`` Activity. Each recurring
        program listed under a venue becomes a separate ``data_type=event``
        Activity.
        """
        activities: list[Activity] = []
        now = datetime.utcnow()

        for venue in raw_items:
            slug = _slugify(venue["name"])
            venue_id = f"curated-{slug}"

            # --- Venue activity -------------------------------------------
            venue_activity = Activity(
                id=venue_id,
                name=venue["name"],
                category=venue["category"],
                experience_type=venue["experience_type"],
                parent_participation=ParentParticipation.required,
                description=f'{venue["name"]} — family-friendly {venue["category"].lower()} in NYC.',
                address=venue["address"],
                lat=venue.get("lat"),
                lng=venue.get("lng"),
                age_min=venue.get("age_min", 0),
                age_max=venue.get("age_max", 12),
                indoor=venue.get("indoor"),
                url=venue["url"],
                source="curated",
                source_id=venue_id,
                data_type=DataType.venue,
                last_updated=now,
            )
            activities.append(venue_activity)

            # --- Recurring program activities -----------------------------
            for prog in venue.get("recurring_programs", []):
                prog_slug = _slugify(prog["name"])
                prog_id = f"curated-{slug}-{prog_slug}"

                schedule_parts = [p for p in [prog.get("day", ""), prog.get("time", "")] if p]
                hours = " | ".join(schedule_parts) if schedule_parts else ""

                prog_activity = Activity(
                    id=prog_id,
                    name=f'{venue["name"]}: {prog["name"]}',
                    category=venue["category"],
                    experience_type=venue["experience_type"],
                    parent_participation=ParentParticipation.required,
                    description=prog.get("description", ""),
                    address=venue["address"],
                    lat=venue.get("lat"),
                    lng=venue.get("lng"),
                    age_min=venue.get("age_min", 0),
                    age_max=venue.get("age_max", 12),
                    indoor=venue.get("indoor"),
                    url=venue["url"],
                    price_display=prog.get("price_display", ""),
                    hours=hours,
                    time_slots=_time_slot_from_str(prog.get("time", "")),
                    source="curated",
                    source_id=prog_id,
                    data_type=DataType.event,
                    last_updated=now,
                )
                activities.append(prog_activity)

        return activities
