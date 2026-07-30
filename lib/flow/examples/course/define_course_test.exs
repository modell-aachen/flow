defmodule Ariadne.Flow.Examples.Course.CommandsTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Examples.Course.DefineCourse
  alias Ariadne.Flow.Examples.Course.Events

  gwt "Defining a course" do
    err("with an existing id",
      given: [%Events.CourseDefined{course_id: "course-1", capacity: 30}],
      when: DefineCourse.command(%{course_id: "course-1", capacity: 25}),
      then: :course_already_exists
    )

    ok("with a new id",
      given: [%Events.CourseDefined{course_id: "course-1", capacity: 30}],
      when: DefineCourse.command(%{course_id: "course-2", capacity: 25}),
      then: [%Events.CourseDefined{course_id: "course-2", capacity: 25}]
    )
  end
end
